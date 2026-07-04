"""Authoritative benchmark metrics, read from the platform's own audit log
(system.query.history) — not from client self-report.

Every benchmark query carries the run marker ``/* <run_tag> tpl=... */`` in its
text, so we filter query.history to exactly one run and COUNT / percentile it.
This is the credibility anchor: the count of 2,000,000 comes straight from
Databricks, and it excludes the analysis queries themselves (they start with
SELECT, not the marker).
"""
from __future__ import annotations

from src.common.config import fq, history_like_pattern


def latest_run_tag(spark, cfg) -> str | None:
    df = spark.sql(f"SELECT run_tag FROM {fq(cfg, 'run_registry')} "
                   f"ORDER BY start_ts DESC LIMIT 1")
    rows = df.collect()
    return rows[0]["run_tag"] if rows else None


def history_metrics(spark, cfg, run_tag: str) -> dict:
    """Headline metrics for one run, straight from system.query.history."""
    pat = history_like_pattern(run_tag)
    sql = f"""
    SELECT
      count(*)                                                   AS total_queries,
      count_if(execution_status = 'FINISHED')                    AS finished,
      count_if(execution_status <> 'FINISHED')                   AS not_finished,
      min(start_time)                                            AS first_start,
      max(end_time)                                              AS last_end,
      unix_timestamp(max(end_time)) - unix_timestamp(min(start_time)) AS span_seconds,
      round(count(*) / nullif(unix_timestamp(max(end_time))
            - unix_timestamp(min(start_time)), 0), 1)            AS achieved_qps,
      percentile_approx(total_duration_ms, 0.50)                 AS p50_total_ms,
      percentile_approx(total_duration_ms, 0.90)                 AS p90_total_ms,
      percentile_approx(total_duration_ms, 0.95)                 AS p95_total_ms,
      percentile_approx(total_duration_ms, 0.99)                 AS p99_total_ms,
      max(total_duration_ms)                                     AS max_total_ms,
      percentile_approx(execution_duration_ms, 0.50)             AS p50_exec_ms,
      percentile_approx(execution_duration_ms, 0.99)             AS p99_exec_ms,
      round(avg(waiting_for_compute_duration_ms), 1)             AS avg_wait_compute_ms,
      round(avg(waiting_at_capacity_duration_ms), 1)             AS avg_wait_capacity_ms,
      round(avg(CASE WHEN from_result_cache THEN 1 ELSE 0 END), 4) AS cache_hit_rate,
      sum(read_bytes)                                            AS total_read_bytes,
      round(avg(read_bytes), 0)                                  AS avg_read_bytes,
      sum(pruned_files)                                          AS sum_pruned_files,
      sum(read_files)                                            AS sum_read_files
    FROM system.query.history
    WHERE statement_text LIKE '{pat}'
    """
    return spark.sql(sql).collect()[0].asDict()


def per_template_metrics(spark, cfg, run_tag: str):
    pat = history_like_pattern(run_tag)
    return spark.sql(f"""
      SELECT regexp_extract(statement_text, 'tpl=([a-zA-Z0-9_]+)', 1) AS template_id,
             count(*)                                    AS queries,
             percentile_approx(total_duration_ms, 0.50)  AS p50_ms,
             percentile_approx(total_duration_ms, 0.99)  AS p99_ms,
             round(avg(CASE WHEN from_result_cache THEN 1 ELSE 0 END), 3) AS cache_hit_rate,
             round(avg(read_bytes), 0)                   AS avg_read_bytes
      FROM system.query.history
      WHERE statement_text LIKE '{pat}'
      GROUP BY 1 ORDER BY queries DESC
    """)


def qps_timeseries(spark, cfg, run_tag: str):
    """Per-second query throughput for the QPS chart."""
    pat = history_like_pattern(run_tag)
    return spark.sql(f"""
      SELECT date_trunc('SECOND', start_time) AS ts, count(*) AS qps
      FROM system.query.history
      WHERE statement_text LIKE '{pat}'
      GROUP BY 1 ORDER BY 1
    """)


def print_report(spark, cfg, run_tag: str) -> dict:
    m = history_metrics(spark, cfg, run_tag)
    tq = m["total_queries"] or 0
    span = m["span_seconds"] or 0
    print("=" * 68)
    print(f"  BENCHMARK RESULT  ·  run_tag = {run_tag}")
    print("=" * 68)
    print(f"  Total queries (system.query.history) : {tq:,}")
    print(f"  Finished / not-finished              : {m['finished']:,} / {m['not_finished']:,}")
    print(f"  Wall span                            : {span:,} s  ({span/60:.1f} min)")
    print(f"  Achieved QPS                         : {m['achieved_qps']}")
    print(f"  Latency total  p50 / p95 / p99 / max : "
          f"{m['p50_total_ms']} / {m['p95_total_ms']} / {m['p99_total_ms']} / {m['max_total_ms']} ms")
    print(f"  Latency exec   p50 / p99             : {m['p50_exec_ms']} / {m['p99_exec_ms']} ms")
    print(f"  Avg wait (compute / capacity)        : "
          f"{m['avg_wait_compute_ms']} / {m['avg_wait_capacity_ms']} ms")
    print(f"  Result-cache hit rate                : {(m['cache_hit_rate'] or 0)*100:.1f} %")
    tb = m["total_read_bytes"] or 0
    print(f"  Bytes read (total / avg per query)   : {tb/1e9:.2f} GB / {(m['avg_read_bytes'] or 0)/1e6:.2f} MB")
    pf, rf = m["sum_pruned_files"] or 0, m["sum_read_files"] or 0
    if pf + rf:
        print(f"  File pruning (pruned / read)         : {pf:,} / {rf:,}  "
              f"({pf/(pf+rf)*100:.1f}% pruned)")
    print("=" * 68)
    return m
