# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Generate the Momos F&B customer-experience datasets
# MAGIC Synthesizes `products`, `customer_profiles`, `customer_orders`,
# MAGIC `customer_reviews` into `<catalog>.<schema>` at the chosen scale.
# MAGIC Runs on **serverless** compute (Spark Connect compatible).

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
dbutils.widgets.text("scale", "")

bundle_root = dbutils.widgets.get("bundle_root") or _find_root(os.getcwd())
sys.path.insert(0, bundle_root)

from src.common.config import get_scale, load_config, schema_fqn  # noqa: E402
from src.data_generation.generate import generate_all  # noqa: E402

# COMMAND ----------
cfg = load_config()
scale_name = dbutils.widgets.get("scale") or cfg.get("active_scale", "sf1")
scale = get_scale(cfg, scale_name)
print(f"Generating scale '{scale_name}' -> {scale} into {schema_fqn(cfg)}")

# COMMAND ----------
counts = generate_all(spark, cfg, scale)
print("Row counts:", counts)
for t, c in counts.items():
    print(f"  {t:20s} {c:,}")

# COMMAND ----------
dbutils.notebook.exit(str(counts))
