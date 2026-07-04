"""Cost of the run, two ways:

  estimate_cost_range()  — available immediately. DBU/hr(size) x clusters x hours
                           x $/DBU. Returns a min/max band (min_clusters..max_clusters)
                           because we don't know the exact autoscale path yet.

  actual_cost()          — authoritative, from system.billing.usage attributed to
                           the warehouse. NOTE: billing.usage can lag by up to a few
                           hours, so run this later for the real dollar figure.

The headline economic metric is $ / million queries — it reframes "2M queries"
as "look how cheap serving is on serverless".
"""
from __future__ import annotations


def dbu_per_hour(cfg, size: str) -> float:
    table = cfg["pricing"]["dbu_per_hour_by_size"]
    if size not in table:
        raise KeyError(f"Unknown warehouse size '{size}'. Known: {list(table)}")
    return float(table[size])


def _price(cfg) -> float:
    return float(cfg["pricing"]["serverless_sql_dollars_per_dbu"])


def estimate_cost(cfg, wall_seconds: float, clusters: float, size: str | None = None) -> dict:
    size = size or cfg["warehouse"]["size"]
    hours = wall_seconds / 3600.0
    dbus = dbu_per_hour(cfg, size) * clusters * hours
    dollars = dbus * _price(cfg)
    return {"size": size, "clusters": clusters, "hours": round(hours, 3),
            "dbus": round(dbus, 2), "dollars": round(dollars, 2)}


def estimate_cost_range(cfg, wall_seconds: float) -> dict:
    """Lower/upper bound on cost from min_clusters..max_clusters."""
    lo = estimate_cost(cfg, wall_seconds, cfg["warehouse"]["min_clusters"])
    hi = estimate_cost(cfg, wall_seconds, cfg["warehouse"]["max_clusters"])
    return {"low": lo, "high": hi}


def actual_cost(spark, cfg, warehouse_id: str, start_ts: str, end_ts: str) -> dict:
    """Authoritative DBUs/$ for the warehouse during [start_ts, end_ts).
    Returns dbus=None if billing.usage hasn't populated yet (it can lag hours)."""
    sql = f"""
      SELECT round(sum(usage_quantity), 4) AS dbus
      FROM system.billing.usage
      WHERE usage_metadata.warehouse_id = '{warehouse_id}'
        AND sku_name ILIKE '%SERVERLESS_SQL_COMPUTE%'
        AND usage_start_time >= '{start_ts}'
        AND usage_start_time <  '{end_ts}'
    """
    dbus = spark.sql(sql).collect()[0]["dbus"]
    if dbus is None:
        return {"dbus": None, "dollars": None,
                "note": "billing.usage not populated yet (can lag a few hours); re-run later"}
    dollars = float(dbus) * _price(cfg)
    return {"dbus": float(dbus), "dollars": round(dollars, 2), "note": "from system.billing.usage"}


def cost_per_million(dollars: float, total_queries: int) -> float:
    if not total_queries:
        return 0.0
    return round(dollars / total_queries * 1_000_000, 2)


def print_cost_report(cfg, wall_seconds, total_queries, actual=None):
    rng = estimate_cost_range(cfg, wall_seconds)
    print("-" * 68)
    print("  COST")
    print("-" * 68)
    lo, hi = rng["low"], rng["high"]
    print(f"  Estimate band ({cfg['warehouse']['size']}, "
          f"{lo['clusters']}..{hi['clusters']} clusters, {lo['hours']} h):")
    print(f"     {lo['dbus']}..{hi['dbus']} DBU  ->  ${lo['dollars']}..${hi['dollars']}")
    if total_queries:
        print(f"     $/million queries: "
              f"${cost_per_million(lo['dollars'], total_queries)}..${cost_per_million(hi['dollars'], total_queries)}")
    if actual and actual.get("dbus") is not None:
        print(f"  ACTUAL (system.billing.usage): {actual['dbus']} DBU -> ${actual['dollars']}")
        if total_queries:
            print(f"     $/million queries (actual): ${cost_per_million(actual['dollars'], total_queries)}")
    elif actual:
        print(f"  ACTUAL: {actual['note']}")
    print("-" * 68)
    return rng
