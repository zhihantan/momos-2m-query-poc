"""Distributed-by-threads load generator.

Runs as a driver-side thread pool on serverless compute. Each thread owns one
SQL-connector connection to the serverless warehouse and issues a paced stream of
parameterized queries until the deadline. Because every query is a network
round-trip, the threads spend almost all their time blocked on the warehouse
(releasing the GIL).

Measured: a single generator node tops out at ~130-150 QPS (Python/connection
overhead). To reach higher aggregate QPS, run several instances sharing one
run_tag (system.query.history aggregates them), or use the executor-distributed
variant (src/workload/distributed.py) — see docs/tuning.md.

Concurrency = num_partitions * threads_per_partition (both kept as config knobs
for readability; physically realized as one thread pool of that size).

Authoritative metrics come from system.query.history (filtered by run tag); this
client log adds client-observed latency and any client-side submission errors.
"""
from __future__ import annotations

import concurrent.futures as cf
import random
import time

from pyspark.sql import functions as F
from pyspark.sql.types import (BooleanType, DoubleType, IntegerType, LongType,
                               StringType, StructField, StructType)

from src.common.config import fq, get_scale, make_run_tag
from src.workload.param_sampler import ParamSampler
from src.workload.query_templates import Tables, TemplatePicker, build_sql

_QUERY_LOG_DDL = """
CREATE TABLE IF NOT EXISTS {tbl} (
  run_tag STRING, worker_id INT, template_id STRING, mode STRING,
  submit_epoch_ms BIGINT, latency_ms DOUBLE, status STRING,
  rows_returned INT, error STRING
) USING delta
"""

_LOG_SCHEMA = StructType([
    StructField("run_tag", StringType()),
    StructField("worker_id", IntegerType()),
    StructField("template_id", StringType()),
    StructField("mode", StringType()),
    StructField("submit_epoch_ms", LongType()),
    StructField("latency_ms", DoubleType()),
    StructField("status", StringType()),
    StructField("rows_returned", IntegerType()),
    StructField("error", StringType()),
])

# Explicit schema so Python ints don't get inferred as long and clash with the
# INT columns in the DDL (Delta refuses to merge int vs long on append).
_REGISTRY_SCHEMA = StructType([
    StructField("run_tag", StringType()),
    StructField("mode", StringType()),
    StructField("scale", StringType()),
    StructField("template_profile", StringType()),
    StructField("warehouse_name", StringType()),
    StructField("concurrency", IntegerType()),
    StructField("target_qps", DoubleType()),
    StructField("duration_seconds", IntegerType()),
    StructField("pace", BooleanType()),
    StructField("start_ts", StringType()),
    StructField("end_ts", StringType()),
    StructField("client_total", LongType()),
    StructField("client_ok", LongType()),
    StructField("client_errors", LongType()),
    StructField("client_qps", DoubleType()),
])

_RUN_REGISTRY_DDL = """
CREATE TABLE IF NOT EXISTS {tbl} (
  run_tag STRING, mode STRING, scale STRING, template_profile STRING,
  warehouse_name STRING, concurrency INT, target_qps DOUBLE,
  duration_seconds INT, pace BOOLEAN, start_ts TIMESTAMP, end_ts TIMESTAMP,
  client_total BIGINT, client_ok BIGINT, client_errors BIGINT, client_qps DOUBLE
) USING delta
"""


def _ensure_result_tables(spark, cfg):
    spark.sql(_QUERY_LOG_DDL.format(tbl=fq(cfg, "query_log")))
    spark.sql(_RUN_REGISTRY_DDL.format(tbl=fq(cfg, "run_registry")))


def _worker_loop(worker_id, cfg, scale, mode, host, http_path, token,
                 template_profile, run_tag, deadline, per_thread_interval,
                 pace, sample_rate, seed):
    """One client thread: open a connection and issue queries until the deadline.

    Cache control uses the connector's persistent session: ``SET use_cached_result
    = false`` in compute mode forces real Photon work on every query; serving mode
    leaves the result cache on and relies on the hot-set params for cache hits.
    (Databricks normalizes out SQL comments when keying the result cache, so a
    unique comment per query does NOT bust it — the session SET is what does.)"""
    from databricks import sql  # installed via %pip in the notebook

    rows, attempts, ok, errors = [], 0, 0, 0

    # Connect inside try so a bad connection records an error and returns fast,
    # instead of hanging the whole thread pool.
    try:
        conn = sql.connect(server_hostname=host, http_path=http_path, access_token=token)
        cur = conn.cursor()
        # session-level cache control (persists for the life of this connection)
        cur.execute("SET use_cached_result = "
                    + ("false" if mode == "compute" else "true"))
    except Exception as e:  # noqa: BLE001
        rows.append((run_tag, worker_id, "CONNECT", mode, int(time.time() * 1000),
                     0.0, "error", 0, repr(e)[:300]))
        return rows, 1, 0, 1

    tables = Tables(cfg)
    picker = TemplatePicker(template_profile)
    sampler = ParamSampler(cfg, scale, mode, seed=seed)
    rng = random.Random(seed)

    # jitter the start so threads don't fire in lockstep
    next_t = time.time() + rng.random() * (per_thread_interval or 0.01)

    while time.time() < deadline:
        tpl = picker.pick(rng)
        params = sampler.sample()
        stmt, binds = build_sql(tpl, tables, params, run_tag)
        submit_ms = int(time.time() * 1000)
        t0 = time.perf_counter()
        status, err, nrows = "ok", None, 0
        try:
            cur.execute(stmt, binds) if binds else cur.execute(stmt)
            nrows = len(cur.fetchall())
        except Exception as e:  # noqa: BLE001 — capture, keep the load running
            status, err = "error", repr(e)[:300]
        latency_ms = (time.perf_counter() - t0) * 1000.0

        attempts += 1
        if status == "ok":
            ok += 1
        else:
            errors += 1
        # sample the ok rows to bound driver memory on huge runs; always keep errors
        if status == "error" or rng.random() < sample_rate:
            rows.append((run_tag, worker_id, tpl.id, mode, submit_ms,
                         latency_ms, status, nrows, err))

        if pace and per_thread_interval > 0:
            next_t += per_thread_interval
            sleep = next_t - time.time()
            if sleep > 0:
                time.sleep(sleep)

    try:
        cur.close(); conn.close()
    except Exception:  # noqa: BLE001
        pass
    return rows, attempts, ok, errors


def run_benchmark(spark, cfg, *, host, http_path, token, overrides=None) -> dict:
    """Execute a benchmark run. `overrides` (from job widgets) beats config."""
    o = overrides or {}
    b = cfg["benchmark"]

    mode = o.get("mode", b["mode"])
    scale_name = o.get("scale", cfg.get("active_scale", "sf1"))
    profile = o.get("template_profile", b.get("template_weights_profile", "mixed"))
    num_partitions = int(o.get("num_partitions", b["num_partitions"]))
    threads_per_partition = int(o.get("threads_per_partition", b["threads_per_partition"]))
    target_qps = float(o.get("target_qps", b["target_qps"]))
    duration_seconds = int(o.get("duration_seconds", b["duration_seconds"]))
    pace = bool(o.get("pace", b.get("pace", True)))
    sample_rate = float(o.get("client_log_sample_rate", b.get("client_log_sample_rate", 1.0)))

    scale = get_scale(cfg, scale_name)
    concurrency = num_partitions * threads_per_partition
    # per-thread interval so that concurrency threads * (1/interval) == target_qps
    per_thread_interval = (concurrency / target_qps) if (pace and target_qps > 0) else 0.0

    run_tag = o.get("run_tag") or make_run_tag(b["run_tag_prefix"], mode, scale_name)
    _ensure_result_tables(spark, cfg)

    print(f"[benchmark] run_tag={run_tag} mode={mode} scale={scale_name} "
          f"concurrency={concurrency} target_qps={target_qps} pace={pace} "
          f"duration={duration_seconds}s profile={profile}")

    start_wall = time.time()
    deadline = start_wall + duration_seconds
    start_ts = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(start_wall))

    all_rows, total, ok_total, err_total = [], 0, 0, 0
    with cf.ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = [
            pool.submit(_worker_loop, wid, cfg, scale, mode, host, http_path, token,
                        profile, run_tag, deadline, per_thread_interval, pace,
                        sample_rate, 1000 + wid)
            for wid in range(concurrency)
        ]
        for fut in cf.as_completed(futures):
            rows, attempts, ok, errors = fut.result()
            all_rows.extend(rows)
            total += attempts; ok_total += ok; err_total += errors

    end_wall = time.time()
    end_ts = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(end_wall))
    wall = max(end_wall - start_wall, 1e-6)
    client_qps = total / wall

    # persist the run-registry row FIRST (so the run is always recorded even if
    # the larger client-log write has trouble). Explicit schema avoids int/long
    # merge conflicts with the INT columns in the DDL.
    reg_row = (run_tag, mode, scale_name, profile, cfg["warehouse"]["name"],
               int(concurrency), float(target_qps), int(duration_seconds), bool(pace),
               start_ts, end_ts, int(total), int(ok_total), int(err_total), float(client_qps))
    (spark.createDataFrame([reg_row], schema=_REGISTRY_SCHEMA)
         .withColumn("start_ts", F.to_timestamp("start_ts"))
         .withColumn("end_ts", F.to_timestamp("end_ts"))
         .write.mode("append").saveAsTable(fq(cfg, "run_registry")))

    # persist the per-query client log in chunks (keeps each createDataFrame under
    # Spark Connect's message-size limit on serverless, and bounds memory)
    if all_rows:
        chunk = 50_000
        for i in range(0, len(all_rows), chunk):
            (spark.createDataFrame(all_rows[i:i + chunk], schema=_LOG_SCHEMA)
                 .write.mode("append").saveAsTable(fq(cfg, "query_log")))

    summary = {
        "run_tag": run_tag, "mode": mode, "scale": scale_name, "concurrency": concurrency,
        "target_qps": target_qps, "duration_seconds": duration_seconds,
        "wall_seconds": round(wall, 1), "client_total": total, "client_ok": ok_total,
        "client_errors": err_total, "client_qps": round(client_qps, 1),
    }
    print(f"[benchmark] DONE {summary}")
    return summary
