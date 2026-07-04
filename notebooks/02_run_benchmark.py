# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Run the benchmark (drive queries at the serverless warehouse)
# MAGIC Driver-side thread-pool load generator. Widgets override config.yaml so
# MAGIC the same notebook runs the 5-minute smoke test and the 60-minute 2M run.
# MAGIC Runs on **serverless** compute; the driver-side thread pool needs only
# MAGIC `databricks-sql-connector`.

# COMMAND ----------
# MAGIC %pip install databricks-sql-connector "pyarrow>=14.0.0" -q
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
import json
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
dbutils.widgets.text("warehouse_id", "")       # REQUIRED: warehouse under test
dbutils.widgets.text("use_smoke", "false")     # true -> use benchmark.smoke defaults
dbutils.widgets.text("mode", "")               # compute | serving
dbutils.widgets.text("scale", "")
dbutils.widgets.text("template_profile", "")   # mixed | serving_heavy | analytics_heavy
dbutils.widgets.text("target_qps", "")
dbutils.widgets.text("duration_seconds", "")
dbutils.widgets.text("num_partitions", "")
dbutils.widgets.text("threads_per_partition", "")
dbutils.widgets.text("pace", "")               # true | false
dbutils.widgets.text("client_log_sample_rate", "")
dbutils.widgets.text("run_tag", "")

bundle_root = dbutils.widgets.get("bundle_root") or _find_root(os.getcwd())
sys.path.insert(0, bundle_root)

from src.common.config import load_config  # noqa: E402
from src.common.dbx import http_path as make_http_path  # noqa: E402
from src.common.dbx import preflight, resolve_host, resolve_token  # noqa: E402
from src.workload.load_generator import run_benchmark  # noqa: E402

# COMMAND ----------
cfg = load_config()

warehouse_id = dbutils.widgets.get("warehouse_id").strip()
assert warehouse_id, "Set the 'warehouse_id' widget to the serverless warehouse under test."

host = resolve_host(spark)
token = resolve_token(dbutils)
http_path = make_http_path(warehouse_id)
print(f"Warehouse {warehouse_id} @ {host}{http_path}")
preflight(host, http_path, token)   # fail fast if the warehouse is unreachable
print("preflight OK")

# COMMAND ----------
# Build overrides from any non-empty widget.
overrides = {}


def _set(key, cast=str):
    v = dbutils.widgets.get(key).strip()
    if v != "":
        overrides[key] = cast(v)


def _bool(v):
    return str(v).lower() in ("1", "true", "yes")


_set("mode"); _set("scale"); _set("template_profile"); _set("run_tag")
_set("target_qps", float); _set("duration_seconds", int)
_set("num_partitions", int); _set("threads_per_partition", int)
_set("pace", _bool); _set("client_log_sample_rate", float)

# Smoke preset: fill any unset knob from benchmark.smoke.
if _bool(dbutils.widgets.get("use_smoke")):
    for k, v in cfg["benchmark"]["smoke"].items():
        overrides.setdefault(k, v)

print("Overrides:", overrides)

# COMMAND ----------
summary = run_benchmark(spark, cfg, host=host, http_path=http_path,
                        token=token, overrides=overrides)
print(json.dumps(summary, indent=2))

# COMMAND ----------
dbutils.notebook.exit(json.dumps(summary))
