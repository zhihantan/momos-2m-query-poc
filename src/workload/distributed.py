"""Spark-executor-distributed load generator.

One Spark job fans per-partition thread pools across **executors** via
``mapInPandas``, so aggregate QPS scales past the single-node ~150 QPS client cap
without launching N separate jobs. Serverless / Spark-Connect compatible (no RDD).

Why it works where a naive port wouldn't: executor Python workers do NOT have the
repo's ``src`` package on their path. So the ``mapInPandas`` worker is
**self-contained** — it imports only ``databricks.sql`` (a pip package installed on
the cluster) plus the stdlib. The driver pre-renders the entire workload into
plain, picklable data (concrete SQL strings + which params are bound) and captures
it in the worker's closure. Nothing from ``src`` is referenced on executors.

Getting real multi-node distribution on serverless: use many partitions
(``num_partitions`` >> one node's cores) and a per-partition run that lasts the
whole ``duration_seconds``. That sustained task backlog is what makes serverless
autoscale executors onto multiple nodes (a short job stays on one node).
"""
from __future__ import annotations

import random
import time

import pandas as pd
from pyspark.sql.types import (DoubleType, IntegerType, LongType, StringType,
                               StructField, StructType)

from src.common.config import fq, get_scale, make_run_tag, run_marker
from src.workload.param_sampler import (_HOT_SEED, DAYS_CHOICES, MIN_REVIEWS_CHOICES,
                                        RATING_THRESHOLDS, REGIONS, TOP_N_CHOICES)
from src.workload.query_templates import TEMPLATES, Tables, WEIGHT_MULTIPLIERS

_RESULT_COLS = ["run_tag", "worker_id", "template_id", "mode", "node",
                "submit_epoch_ms", "latency_ms", "status", "rows_returned", "error"]

_RESULT_SCHEMA = StructType([
    StructField("run_tag", StringType()),
    StructField("worker_id", IntegerType()),
    StructField("template_id", StringType()),
    StructField("mode", StringType()),
    StructField("node", StringType()),
    StructField("submit_epoch_ms", LongType()),
    StructField("latency_ms", DoubleType()),
    StructField("status", StringType()),
    StructField("rows_returned", IntegerType()),
    StructField("error", StringType()),
])

_DIST_LOG_DDL = """
CREATE TABLE IF NOT EXISTS {tbl} (
  run_tag STRING, worker_id INT, template_id STRING, mode STRING, node STRING,
  submit_epoch_ms BIGINT, latency_ms DOUBLE, status STRING,
  rows_returned INT, error STRING
) USING delta
"""


def _render_specs(cfg, mode, run_tag, profile):
    """Driver-side: expand every template into concrete SQL specs (plain data).

    Each spec = (template_id, sql_text_with_bind_markers, bind_keys_tuple). High-
    cardinality keys stay as ``:name`` bind markers (sampled per query on the
    executor); low-cardinality params are baked into concrete literals here. In
    serving mode we bake the hot singletons so the text repeats (cache hits); in
    compute mode we expand the full cross-product for variety."""
    T = Tables(cfg)
    row_limit = cfg["benchmark"]["result_row_limit"]
    mult = WEIGHT_MULTIPLIERS[profile]
    if mode == "serving":
        regions, days = [REGIONS[0]], [30]
        minrs, topns, rths = [MIN_REVIEWS_CHOICES[0]], [TOP_N_CHOICES[0]], [RATING_THRESHOLDS[0]]
    else:
        regions, days = REGIONS, DAYS_CHOICES
        minrs, topns, rths = MIN_REVIEWS_CHOICES, TOP_N_CHOICES, RATING_THRESHOLDS

    specs, weights = [], []
    for t in TEMPLATES:
        rendered = []
        for rg in regions:
            for dy in days:
                for mr in minrs:
                    for tn in topns:
                        for rt in rths:
                            p = {"region": rg, "days": dy, "min_reviews": mr,
                                 "top_n": tn, "rating_threshold": rt, "row_limit": row_limit}
                            sqltext = t.sql(T, p)
                            if sqltext not in rendered:
                                rendered.append(sqltext)
        w = (t.weight * mult[t.category]) / len(rendered)
        for sqltext in rendered:
            specs.append((t.id, f"{run_marker(run_tag, t.id)} {sqltext}", tuple(t.params)))
            weights.append(w)
    cum, running = [], 0.0
    for w in weights:
        running += w
        cum.append(running)
    return specs, cum, running


def _ensure_dist_table(spark, cfg):
    spark.sql(_DIST_LOG_DDL.format(tbl=fq(cfg, "query_log_dist")))


def run_benchmark_distributed(spark, cfg, *, host, http_path, token, overrides=None) -> dict:
    """Execute the load across Spark executors. Authoritative totals still come
    from system.query.history (filtered by run_tag); this returns the client-side
    view plus the number of distinct executor nodes that drove the load."""
    o = overrides or {}
    b = cfg["benchmark"]
    mode = o.get("mode", b["mode"])
    scale_name = o.get("scale", cfg.get("active_scale", "sf1"))
    profile = o.get("template_profile", b.get("template_weights_profile", "mixed"))
    num_partitions = int(o.get("num_partitions", 96))
    threads_per_partition = int(o.get("threads_per_partition", 4))
    duration_seconds = int(o.get("duration_seconds", b["duration_seconds"]))
    target_qps = float(o.get("target_qps", b["target_qps"]))
    pace = bool(o.get("pace", b.get("pace", True)))
    sample_rate = float(o.get("client_log_sample_rate", b.get("client_log_sample_rate", 1.0)))
    run_tag = o.get("run_tag") or make_run_tag(b["run_tag_prefix"] + "_dist", mode, scale_name)

    scale = get_scale(cfg, scale_name)
    specs, cum, total_w = _render_specs(cfg, mode, run_tag, profile)
    concurrency = num_partitions * threads_per_partition
    per_thread_interval = (concurrency / target_qps) if (pace and target_qps > 0) else 0.0

    # plain-data bind sources (captured in the closure)
    hot = int(b["hot_set_size"])
    hrng = random.Random(_HOT_SEED)
    n_cust, n_prod, n_ord = int(scale["profiles"]), int(scale["products"]), int(scale["orders"])
    hot_customers = [hrng.randrange(n_cust) for _ in range(hot)]
    hot_products = [hrng.randrange(n_prod) for _ in range(hot)]
    hot_orders = [hrng.randrange(n_ord) for _ in range(hot)]

    _ensure_dist_table(spark, cfg)
    print(f"[dist] run_tag={run_tag} mode={mode} scale={scale_name} partitions={num_partitions} "
          f"threads/part={threads_per_partition} concurrency={concurrency} "
          f"target_qps={target_qps} pace={pace} duration={duration_seconds}s specs={len(specs)}")

    import bisect

    def worker(iterator):
        import concurrent.futures as cf
        import random as R
        import socket
        import time as TM
        from databricks import sql

        node = socket.gethostname()

        def one_thread(wid, seed):
            rng = R.Random(seed)
            rows, attempts, ok, errors = [], 0, 0, 0
            try:
                conn = sql.connect(server_hostname=host, http_path=http_path, access_token=token)
                cur = conn.cursor()
                cur.execute("SET use_cached_result = " + ("false" if mode == "compute" else "true"))
            except Exception as e:  # noqa: BLE001
                return ([(run_tag, wid, "CONNECT", mode, node, int(TM.time() * 1000),
                          0.0, "error", 0, repr(e)[:300])], 1, 0, 1)
            dl = TM.time() + duration_seconds
            nxt = TM.time() + rng.random() * (per_thread_interval or 0.01)
            while TM.time() < dl:
                x = rng.random() * total_w
                tid, text, bind_keys = specs[bisect.bisect_left(cum, x)]
                binds = {}
                for k in bind_keys:
                    if k == "customer_id":
                        binds[k] = rng.choice(hot_customers) if mode == "serving" else rng.randrange(n_cust)
                    elif k == "product_id":
                        binds[k] = rng.choice(hot_products) if mode == "serving" else rng.randrange(n_prod)
                    elif k == "order_id":
                        binds[k] = rng.choice(hot_orders) if mode == "serving" else rng.randrange(n_ord)
                submit = int(TM.time() * 1000)
                t0 = TM.perf_counter()
                status, err, nr = "ok", None, 0
                try:
                    cur.execute(text, binds) if binds else cur.execute(text)
                    nr = len(cur.fetchall())
                except Exception as e:  # noqa: BLE001
                    status, err = "error", repr(e)[:300]
                lat = (TM.perf_counter() - t0) * 1000.0
                attempts += 1
                if status == "ok":
                    ok += 1
                else:
                    errors += 1
                if status == "error" or rng.random() < sample_rate:
                    rows.append((run_tag, wid, tid, mode, node, submit, lat, status, nr, err))
                if pace and per_thread_interval > 0:
                    nxt += per_thread_interval
                    slp = nxt - TM.time()
                    if slp > 0:
                        TM.sleep(slp)
            try:
                cur.close(); conn.close()
            except Exception:  # noqa: BLE001
                pass
            return rows, attempts, ok, errors

        for pdf in iterator:
            out = []
            for r in pdf.itertuples():
                base = int(r.wid)
                with cf.ThreadPoolExecutor(max_workers=threads_per_partition) as ex:
                    futs = [ex.submit(one_thread, base * 1000 + i, base * 100003 + i)
                            for i in range(threads_per_partition)]
                    for f in cf.as_completed(futs):
                        rows, _a, _o, _e = f.result()
                        out.extend(rows)
            yield pd.DataFrame(out, columns=_RESULT_COLS) if out else pd.DataFrame(columns=_RESULT_COLS)

    workers_df = (spark.range(num_partitions).withColumnRenamed("id", "wid")
                  .repartition(num_partitions))
    start_wall = time.time()
    (workers_df.mapInPandas(worker, schema=_RESULT_SCHEMA)
        .write.mode("append").saveAsTable(fq(cfg, "query_log_dist")))
    wall = time.time() - start_wall

    # client-side view from the distributed log (authoritative totals: query.history)
    agg = spark.sql(f"""
      SELECT count(*) logged, count(DISTINCT node) nodes,
             count_if(status <> 'ok') errors,
             (unix_timestamp(max(timestamp_millis(submit_epoch_ms)))
              - unix_timestamp(min(timestamp_millis(submit_epoch_ms)))) span_s
      FROM {fq(cfg, 'query_log_dist')} WHERE run_tag = '{run_tag}'
    """).collect()[0].asDict()

    # Register the run so the dashboard features it (query.history stays authoritative).
    # Totals are estimated from the sampled log: logged / sample_rate.
    est_total = int(round((agg["logged"] or 0) / max(sample_rate, 1e-9)))
    est_qps = round(est_total / agg["span_s"], 1) if agg["span_s"] else 0.0
    try:
        spark.sql(f"""
          INSERT INTO {fq(cfg, 'run_registry')}
          SELECT '{run_tag}', '{mode}', '{scale_name}', '{profile}',
                 '{cfg['warehouse']['name']}', CAST({concurrency} AS INT),
                 CAST({target_qps} AS DOUBLE), CAST({duration_seconds} AS INT),
                 {str(pace).lower()},
                 min(timestamp_millis(submit_epoch_ms)), max(timestamp_millis(submit_epoch_ms)),
                 CAST({est_total} AS BIGINT), CAST({est_total - (agg['errors'] or 0)} AS BIGINT),
                 CAST({agg['errors'] or 0} AS BIGINT), CAST({est_qps} AS DOUBLE)
          FROM {fq(cfg, 'query_log_dist')} WHERE run_tag = '{run_tag}'
        """)
    except Exception as e:  # noqa: BLE001
        print("[dist] run_registry write skipped:", repr(e)[:200])

    summary = {"run_tag": run_tag, "mode": mode, "scale": scale_name,
               "partitions": num_partitions, "threads_per_partition": threads_per_partition,
               "concurrency": concurrency, "wall_seconds": round(wall, 1),
               "logged_rows": agg["logged"], "distinct_nodes": agg["nodes"],
               "errors": agg["errors"], "log_span_s": agg["span_s"],
               "note": "authoritative count/QPS/latency: system.query.history filtered by run_tag"}
    print(f"[dist] DONE {summary}")
    return summary
