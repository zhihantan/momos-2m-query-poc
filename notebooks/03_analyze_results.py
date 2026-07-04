# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Analyze results — the proof
# MAGIC Reads **system.query.history** (authoritative count + latency + cache) and
# MAGIC **system.billing.usage** (actual DBUs/$) for a run. Defaults to the latest run.

# COMMAND ----------
import os
import sys


def _find_root(start):
    d = start
    for _ in range(8):
        if os.path.exists(os.path.join(d, "src", "config", "config.yaml")):
            return d
        d = os.path.dirname(d)
    return start


dbutils.widgets.text("bundle_root", "")
dbutils.widgets.text("run_tag", "")            # blank -> latest run
dbutils.widgets.text("warehouse_id", "")       # enables actual $ from billing.usage

bundle_root = dbutils.widgets.get("bundle_root") or _find_root(os.getcwd())
sys.path.insert(0, bundle_root)

from src.analysis.cost import actual_cost, print_cost_report  # noqa: E402
from src.analysis.metrics import (  # noqa: E402
    latest_run_tag, per_template_metrics, print_report, qps_timeseries)
from src.common.config import fq, load_config  # noqa: E402

# COMMAND ----------
cfg = load_config()
run_tag = dbutils.widgets.get("run_tag").strip() or latest_run_tag(spark, cfg)
assert run_tag, "No run found. Run 02_run_benchmark first."
print("Analyzing run:", run_tag)

# COMMAND ----------
# Headline metrics — straight from the platform's audit log.
m = print_report(spark, cfg, run_tag)

# COMMAND ----------
# Per-template breakdown
display(per_template_metrics(spark, cfg, run_tag))

# COMMAND ----------
# QPS over time
display(qps_timeseries(spark, cfg, run_tag))

# COMMAND ----------
# Cost — estimate band now, actual from billing.usage (may lag a few hours)
reg = spark.sql(f"SELECT * FROM {fq(cfg, 'run_registry')} "
                f"WHERE run_tag = '{run_tag}'").collect()[0]
warehouse_id = dbutils.widgets.get("warehouse_id").strip()
actual = None
if warehouse_id:
    actual = actual_cost(spark, cfg, warehouse_id,
                         str(reg["start_ts"]), str(reg["end_ts"]))
span = m["span_seconds"] or (reg["end_ts"] - reg["start_ts"]).total_seconds()
print_cost_report(cfg, span, m["total_queries"], actual)
