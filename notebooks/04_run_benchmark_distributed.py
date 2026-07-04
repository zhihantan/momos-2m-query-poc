# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Run the benchmark — distributed across executors (single job)
# MAGIC Fans per-partition thread pools across Spark **executors** via mapInPandas,
# MAGIC so one job scales past the single-node ~150 QPS client cap. Serverless
# MAGIC compute; the connector is installed on every worker via `%pip`.

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
dbutils.widgets.text("warehouse_id", "")
dbutils.widgets.text("mode", "serving")            # serving | compute
dbutils.widgets.text("scale", "full")
dbutils.widgets.text("template_profile", "mixed")
dbutils.widgets.text("num_partitions", "24")       # ~1 node's cores on serverless (1 wave); raise >> cores on classic multi-node
dbutils.widgets.text("threads_per_partition", "8")
dbutils.widgets.text("duration_seconds", "600")
dbutils.widgets.text("target_qps", "1000")
dbutils.widgets.text("pace", "false")              # false = unleashed (find max QPS)
dbutils.widgets.text("client_log_sample_rate", "0.05")
dbutils.widgets.text("run_tag", "")

bundle_root = dbutils.widgets.get("bundle_root") or _find_root(os.getcwd())
sys.path.insert(0, bundle_root)

from src.common.config import load_config  # noqa: E402
from src.common.dbx import http_path as make_http_path  # noqa: E402
from src.common.dbx import preflight, resolve_host, resolve_token  # noqa: E402
from src.workload.distributed import run_benchmark_distributed  # noqa: E402

# COMMAND ----------
cfg = load_config()
warehouse_id = dbutils.widgets.get("warehouse_id").strip()
assert warehouse_id, "Set the 'warehouse_id' widget."
host = resolve_host(spark)
token = resolve_token(dbutils)
http_path = make_http_path(warehouse_id)
preflight(host, http_path, token)
print("preflight OK ·", host, http_path)

# COMMAND ----------
overrides = {}


def _set(k, cast=str):
    v = dbutils.widgets.get(k).strip()
    if v != "":
        overrides[k] = cast(v)


def _bool(v):
    return str(v).lower() in ("1", "true", "yes")


_set("mode"); _set("scale"); _set("template_profile"); _set("run_tag")
_set("num_partitions", int); _set("threads_per_partition", int)
_set("duration_seconds", int); _set("target_qps", float)
_set("pace", _bool); _set("client_log_sample_rate", float)
print("overrides:", overrides)

# COMMAND ----------
summary = run_benchmark_distributed(spark, cfg, host=host, http_path=http_path,
                                    token=token, overrides=overrides)
print(json.dumps(summary, indent=2, default=str))
dbutils.notebook.exit(json.dumps(summary, default=str))
