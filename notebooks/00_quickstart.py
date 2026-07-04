# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Quickstart (interactive)
# MAGIC End-to-end in one notebook at the cheap **sf1** scale: generate data →
# MAGIC run a short benchmark → see the proof. Attach to any cluster/serverless
# MAGIC and Run All. For the full 2M/60-min run, deploy the bundle and use the
# MAGIC `momos_benchmark` job instead (see README).

# COMMAND ----------
# MAGIC %pip install databricks-sql-connector "pyarrow>=14.0.0" databricks-sdk pyyaml -q
# MAGIC dbutils.library.restartPython()

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


bundle_root = _find_root(os.getcwd())
sys.path.insert(0, bundle_root)

from databricks.sdk import WorkspaceClient  # noqa: E402
from src.analysis.metrics import print_report  # noqa: E402
from src.analysis.cost import print_cost_report  # noqa: E402
from src.common.config import get_scale, load_config, schema_fqn  # noqa: E402
from src.data_generation.generate import generate_all  # noqa: E402
from src.workload.load_generator import run_benchmark  # noqa: E402

cfg = load_config()

# COMMAND ----------
# 1) Generate sf1 data (cheap)
scale = get_scale(cfg, "sf1")
print(f"Generating sf1 -> {scale} into {schema_fqn(cfg)}")
print("Row counts:", generate_all(spark, cfg, scale))

# COMMAND ----------
# 2) Find (by name) the serverless warehouse under test
w = WorkspaceClient()
wh = next((x for x in w.warehouses.list() if x.name == cfg["warehouse"]["name"]), None)
assert wh, (f"Warehouse '{cfg['warehouse']['name']}' not found. Deploy the bundle "
            f"(creates it) or set warehouse.name in config.yaml to an existing one.")
warehouse_id = wh.id
host = spark.conf.get("spark.databricks.workspaceUrl")
token = (dbutils.notebook.entry_point.getDbutils().notebook()
         .getContext().apiToken().get())
print(f"Using warehouse {wh.name} ({warehouse_id})")

# COMMAND ----------
# 3) Short smoke benchmark against sf1 data
summary = run_benchmark(
    spark, cfg, host=host, http_path=f"/sql/1.0/warehouses/{warehouse_id}",
    token=token,
    overrides={"scale": "sf1", "mode": "compute",
               "duration_seconds": 120, "target_qps": 40,
               "num_partitions": 8, "threads_per_partition": 4})
print(summary)

# COMMAND ----------
# 4) The proof — from system.query.history + a cost estimate
import time
time.sleep(10)  # give query.history a moment to settle
m = print_report(spark, cfg, summary["run_tag"])
print_cost_report(cfg, m["span_seconds"] or summary["wall_seconds"], m["total_queries"])
