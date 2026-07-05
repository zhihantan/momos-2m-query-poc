"""Synthetic data generation for the Momos multi-location F&B customer-experience
star schema.

Everything is expressed with the Spark DataFrame API + pyspark.sql.functions so
it runs on **serverless** compute (Spark Connect compatible — no RDD, no UDFs).
Row generation is fully distributed via ``spark.range`` + column expressions, so
tens of millions of rows synthesize in minutes.

Tables (in ``<catalog>.<schema>``):
    products           dim  — menu items (name, category, brand, price)
    customer_profiles  dim  — one row per customer (loyalty, LTV, geo, channel)
    customer_orders    fact — one row per order (amounts, tip, status, store)
    customer_reviews   fact — one row per review (rating, sentiment, text)

Design choices that make the demo realistic:
  * Referential integrity: orders/reviews reference real customer_ids/product_ids.
  * Power-law customer activity: a minority of customers place most orders (skew
    stresses the optimizer the way real data does).
  * Correlated rating <-> sentiment <-> helpful_votes so analytical queries return
    believable answers.
  * Liquid clustering on the join/lookup keys (customer_id / product_id) so point
    lookups and joins prune hard on the serverless warehouse.
"""
from __future__ import annotations

from pyspark.sql import functions as F

# --- reference data (kept small; used to build SQL array literals) ----------

CATEGORIES = ["Burgers", "Pizza", "Sushi", "Tacos", "Salads", "Beverages",
              "Desserts", "Sides", "Breakfast", "Coffee"]
BRANDS = ["Momos Kitchen", "Momos Express", "Momos Grill", "Momos Cafe",
          "Momos Street", "Momos Bowl"]
CHANNELS = ["App", "Web", "InStore", "Delivery", "Kiosk"]
PAYMENTS = ["Card", "ApplePay", "GooglePay", "Cash", "GiftCard"]
DEVICES = ["iOS", "Android", "Web"]
AGE_BANDS = ["18-24", "25-34", "35-44", "45-54", "55-64", "65+"]
PROMOS = ["", "", "", "WELCOME10", "FREESHIP", "BOGO", "LOYAL15", "APPONLY5"]

# (city, region, country) kept aligned by index
LOCATIONS = [
    ("Singapore", "APAC", "SG"), ("Kuala Lumpur", "APAC", "MY"),
    ("Jakarta", "APAC", "ID"), ("Manila", "APAC", "PH"),
    ("Bangkok", "APAC", "TH"), ("Sydney", "APAC", "AU"),
    ("San Francisco", "AMER", "US"), ("New York", "AMER", "US"),
    ("Austin", "AMER", "US"), ("Toronto", "AMER", "CA"),
    ("London", "EMEA", "GB"), ("Dubai", "EMEA", "AE"),
]

REVIEW_BODY = {
    "positive": ["Absolutely delicious, will order again!",
                 "Great food and super fast delivery.",
                 "Best in town — highly recommend.",
                 "Fresh, hot, and packed with flavor.",
                 "Fantastic service and generous portions."],
    "neutral":  ["It was okay, nothing special.",
                 "Decent but a little pricey.",
                 "Average experience overall.",
                 "Food was fine, delivery was slow."],
    "negative": ["Arrived cold and late, disappointed.",
                 "Order was wrong again.",
                 "Not worth the price at all.",
                 "Quality has really dropped.",
                 "Won't be ordering from here again."],
}
REVIEW_TITLE = {
    "positive": ["Loved it", "10/10", "New favorite", "So good"],
    "neutral":  ["It's fine", "Just okay", "Meh"],
    "negative": ["Disappointed", "Never again", "Poor"],
}


# --- small helpers ----------------------------------------------------------

def _arr(values):
    return F.array(*[F.lit(v) for v in values])


def _rand_pick(values, seed):
    """Uniform pick from a python list, as a Spark column."""
    arr = _arr(values)
    idx = (F.floor(F.rand(seed) * len(values)) + 1).cast("int")
    return F.element_at(arr, idx)


def _skewed_customer_id(n_customers, seed):
    """Power-law-ish: rand^2 concentrates orders on low-id customers, so a
    minority of customers accumulate most of the activity."""
    return F.least(
        F.lit(n_customers - 1),
        F.floor(F.pow(F.rand(seed), F.lit(2.0)) * n_customers),
    ).cast("long")


def schema_fqn(cfg):
    return f"{cfg['databricks']['catalog']}.{cfg['databricks']['schema']}"


def _t(cfg, key):
    return f"{schema_fqn(cfg)}.{cfg['tables'][key]}"


def _write_clustered(spark, df, table, cluster_cols):
    """Overwrite a Delta table, set liquid clustering on the given keys, and
    OPTIMIZE so the clustering takes effect (helps point lookups + joins prune)."""
    (df.write.format("delta").mode("overwrite")
       .option("overwriteSchema", "true").saveAsTable(table))
    spark.sql(f"ALTER TABLE {table} CLUSTER BY ({', '.join(cluster_cols)})")
    spark.sql(f"OPTIMIZE {table}")


# --- generators -------------------------------------------------------------

def ensure_schema(spark, cfg):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_fqn(cfg)}")


def generate_products(spark, cfg, n):
    df = (spark.range(n).withColumnRenamed("id", "product_id")
          .withColumn("category", _rand_pick(CATEGORIES, 11))
          .withColumn("brand", _rand_pick(BRANDS, 12))
          .withColumn("product_name",
                      F.concat_ws(" ", _rand_pick(
                          ["Classic", "Spicy", "Deluxe", "Mini", "Double",
                           "Grilled", "Crispy", "Signature"], 13),
                          F.col("category")))
          .withColumn("base_price",
                      F.round(F.lit(3.0) + F.rand(14) * 27.0, 2).cast("decimal(8,2)"))
          .withColumn("is_active", (F.rand(15) < 0.95)))
    _write_clustered(spark, df, _t(cfg, "products"), ["product_id"])
    return spark.table(_t(cfg, "products")).count()


def generate_profiles(spark, cfg, n):
    loc_idx = (F.floor(F.rand(21) * len(LOCATIONS)) + 1).cast("int")
    cities = _arr([l[0] for l in LOCATIONS])
    regions = _arr([l[1] for l in LOCATIONS])
    countries = _arr([l[2] for l in LOCATIONS])
    # loyalty tier skew: mostly Bronze, few Platinum
    r = F.rand(22)
    tier = (F.when(r < 0.60, "Bronze").when(r < 0.85, "Silver")
             .when(r < 0.97, "Gold").otherwise("Platinum"))
    df = (spark.range(n).withColumnRenamed("id", "customer_id")
          .withColumn("_loc", loc_idx)
          .withColumn("full_name", F.concat(F.lit("Customer "), F.col("customer_id")))
          .withColumn("email", F.concat(F.lit("cust"), F.col("customer_id"),
                                         F.lit("@example.com")))
          .withColumn("signup_date",
                      F.expr("date_sub(current_date(), CAST(rand()*1460 AS INT))"))
          .withColumn("city", F.element_at(cities, F.col("_loc")))
          .withColumn("region", F.element_at(regions, F.col("_loc")))
          .withColumn("country", F.element_at(countries, F.col("_loc")))
          .withColumn("age_band", _rand_pick(AGE_BANDS, 23))
          .withColumn("loyalty_tier", tier)
          .withColumn("marketing_opt_in", (F.rand(24) < 0.55))
          .withColumn("preferred_channel", _rand_pick(CHANNELS, 25))
          .withColumn("device", _rand_pick(DEVICES, 26))
          .withColumn("lifetime_value",
                      F.round((F.rand(27) * 2000.0)
                              * F.when(F.col("loyalty_tier") == "Platinum", 4.0)
                                 .when(F.col("loyalty_tier") == "Gold", 2.5)
                                 .when(F.col("loyalty_tier") == "Silver", 1.5)
                                 .otherwise(1.0), 2).cast("decimal(12,2)"))
          .drop("_loc"))
    _write_clustered(spark, df, _t(cfg, "profiles"), ["customer_id"])
    return spark.table(_t(cfg, "profiles")).count()


def generate_orders(spark, cfg, n, n_customers, n_stores=400):
    r_status = F.rand(31)
    status = (F.when(r_status < 0.88, "COMPLETED").when(r_status < 0.93, "CANCELLED")
               .when(r_status < 0.96, "REFUNDED").otherwise("PENDING"))
    num_items = (F.floor(F.pow(F.rand(32), F.lit(1.5)) * 8) + 1).cast("int")
    subtotal = F.round(num_items * (F.lit(5.0) + F.rand(33) * 22.0), 2)
    discount = F.round(F.when(F.rand(34) < 0.30, subtotal * F.rand(35) * 0.25)
                        .otherwise(F.lit(0.0)), 2)
    taxable = subtotal - discount
    tax = F.round(taxable * 0.08, 2)
    channel = _rand_pick(CHANNELS, 36)
    tip = F.round(F.when(channel.isin("Delivery", "InStore"),
                         taxable * F.rand(37) * 0.20).otherwise(F.lit(0.0)), 2)
    df = (spark.range(n).withColumnRenamed("id", "order_id")
          .withColumn("customer_id", _skewed_customer_id(n_customers, 38))
          .withColumn("store_id", (F.floor(F.rand(39) * n_stores)).cast("int"))
          .withColumn("region", _rand_pick([l[1] for l in LOCATIONS], 40))
          .withColumn("channel", channel)
          .withColumn("order_ts", F.expr(
              "current_timestamp() - make_interval(0,0,0, CAST(rand()*730 AS INT), "
              "CAST(rand()*15 + 8 AS INT), CAST(rand()*60 AS INT), 0)"))
          .withColumn("status", status)
          .withColumn("num_items", num_items)
          .withColumn("subtotal", subtotal.cast("decimal(10,2)"))
          .withColumn("discount", discount.cast("decimal(10,2)"))
          .withColumn("tax", tax.cast("decimal(10,2)"))
          .withColumn("tip", tip.cast("decimal(10,2)"))
          .withColumn("total_amount", F.round(taxable + tax + tip, 2).cast("decimal(10,2)"))
          .withColumn("payment_method", _rand_pick(PAYMENTS, 41))
          .withColumn("promo_code",
                      F.nullif(_rand_pick(PROMOS, 42), F.lit(""))))
    _write_clustered(spark, df, _t(cfg, "orders"), ["customer_id"])
    return spark.table(_t(cfg, "orders")).count()


def generate_reviews(spark, cfg, n, n_customers, n_products, n_orders, n_stores=400):
    # rating skew toward 4-5, with a detractor tail
    rr = F.rand(51)
    rating = (F.when(rr < 0.50, 5).when(rr < 0.75, 4).when(rr < 0.86, 3)
               .when(rr < 0.94, 2).otherwise(1)).cast("int")
    sentiment = (F.when(rating >= 4, "positive").when(rating == 3, "neutral")
                  .otherwise("negative"))

    def _body_for(kind):
        return _rand_pick(REVIEW_BODY[kind], 52)

    def _title_for(kind):
        return _rand_pick(REVIEW_TITLE[kind], 53)

    body = (F.when(sentiment == "positive", _body_for("positive"))
             .when(sentiment == "neutral", _body_for("neutral"))
             .otherwise(_body_for("negative")))
    title = (F.when(sentiment == "positive", _title_for("positive"))
              .when(sentiment == "neutral", _title_for("neutral"))
              .otherwise(_title_for("negative")))
    # helpful_votes: extreme ratings (1 & 5) attract more votes
    helpful = (F.floor(F.rand(54) * F.when(rating.isin(1, 5), 60).otherwise(12))).cast("int")
    df = (spark.range(n).withColumnRenamed("id", "review_id")
          .withColumn("customer_id", _skewed_customer_id(n_customers, 55))
          .withColumn("order_id", (F.floor(F.rand(56) * n_orders)).cast("long"))
          .withColumn("product_id", (F.floor(F.rand(57) * n_products)).cast("long"))
          .withColumn("store_id", (F.floor(F.rand(58) * n_stores)).cast("int"))
          .withColumn("region", _rand_pick([l[1] for l in LOCATIONS], 59))
          .withColumn("rating", rating)
          .withColumn("sentiment", sentiment)
          .withColumn("review_ts", F.expr(
              "current_timestamp() - make_interval(0,0,0, CAST(rand()*730 AS INT), "
              "CAST(rand()*24 AS INT), CAST(rand()*60 AS INT), 0)"))
          .withColumn("title", title)
          .withColumn("body", body)
          .withColumn("verified_purchase", (F.rand(60) < 0.80))
          .withColumn("helpful_votes", helpful))
    _write_clustered(spark, df, _t(cfg, "reviews"), ["customer_id"])
    return spark.table(_t(cfg, "reviews")).count()


def generate_all(spark, cfg, scale):
    """Generate every table for the given scale dict {products,profiles,orders,reviews}.
    Returns a dict of row counts."""
    ensure_schema(spark, cfg)
    counts = {}
    counts["products"] = generate_products(spark, cfg, scale["products"])
    counts["profiles"] = generate_profiles(spark, cfg, scale["profiles"])
    counts["orders"] = generate_orders(spark, cfg, scale["orders"], scale["profiles"])
    counts["reviews"] = generate_reviews(
        spark, cfg, scale["reviews"], scale["profiles"],
        scale["products"], scale["orders"])
    return counts
