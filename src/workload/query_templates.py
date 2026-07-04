"""The benchmark query workload: 16 parameterized F&B customer-experience
templates spanning the full latency spectrum, from single-row point lookups to
cross-dataset joins.

Every template:
  * is bounded by a LIMIT (we measure query-engine throughput, not result
    transfer),
  * takes its literals from the ParamSampler (see param_sampler.py), whose
    distribution decides whether the result cache is busted (compute mode) or
    exploited (serving mode),
  * is prefixed at execution time with a run-tag SQL comment so the query is
    identifiable in system.query.history.

Categories: serving | aggregation | topn | window | join.
"""
from __future__ import annotations

import bisect
from dataclasses import dataclass
from typing import Callable

from src.common.config import run_marker


class Tables:
    """Fully-qualified table names resolved from config."""

    def __init__(self, cfg: dict):
        s = f"{cfg['databricks']['catalog']}.{cfg['databricks']['schema']}"
        self.profiles = f"{s}.{cfg['tables']['profiles']}"
        self.orders = f"{s}.{cfg['tables']['orders']}"
        self.reviews = f"{s}.{cfg['tables']['reviews']}"
        self.products = f"{s}.{cfg['tables']['products']}"


@dataclass(frozen=True)
class Template:
    id: str
    category: str
    weight: int                       # base weight for the "mixed" profile
    sql: Callable[[Tables, dict], str]
    # High-cardinality keys bound as query parameters (":name") so the plan
    # compiles ONCE and is reused across millions of executions. Low-cardinality
    # params (region, days, top_n, ...) stay literals: only a few distinct plans.
    params: tuple = ()


# --- SQL builders (one per template) ---------------------------------------
# Point lookups / serving --------------------------------------------------

def _q01(T, p):  # profile by id (single row)
    return (f"SELECT customer_id, full_name, loyalty_tier, lifetime_value, city, region "
            f"FROM {T.profiles} WHERE customer_id = :customer_id")


def _q02(T, p):  # order by id (single row)
    return (f"SELECT order_id, customer_id, order_ts, status, total_amount, channel "
            f"FROM {T.orders} WHERE order_id = :order_id")


def _q03(T, p):  # a customer's most recent orders
    return (f"SELECT order_id, order_ts, status, total_amount, channel "
            f"FROM {T.orders} WHERE customer_id = :customer_id "
            f"ORDER BY order_ts DESC LIMIT {p['row_limit']}")


def _q04(T, p):  # latest reviews for a menu item
    return (f"SELECT review_id, customer_id, rating, review_ts, title, helpful_votes "
            f"FROM {T.reviews} WHERE product_id = :product_id "
            f"ORDER BY review_ts DESC LIMIT {p['row_limit']}")


def _q05(T, p):  # a customer's review history
    return (f"SELECT review_id, product_id, rating, review_ts, title "
            f"FROM {T.reviews} WHERE customer_id = :customer_id "
            f"ORDER BY review_ts DESC LIMIT {p['row_limit']}")


def _q06(T, p):  # customer-360: profile + order & review rollups for one customer
    return (f"SELECT p.customer_id, p.loyalty_tier, p.lifetime_value, "
            f"(SELECT count(*) FROM {T.orders} o WHERE o.customer_id = p.customer_id) AS lifetime_orders, "
            f"(SELECT round(avg(r.rating),2) FROM {T.reviews} r WHERE r.customer_id = p.customer_id) AS avg_rating "
            f"FROM {T.profiles} p WHERE p.customer_id = :customer_id")


# Aggregations -------------------------------------------------------------

def _q07(T, p):  # avg rating by menu category
    return (f"SELECT pr.category, count(*) AS reviews, round(avg(r.rating),3) AS avg_rating "
            f"FROM {T.reviews} r JOIN {T.products} pr ON r.product_id = pr.product_id "
            f"GROUP BY pr.category ORDER BY avg_rating DESC LIMIT {p['row_limit']}")


def _q08(T, p):  # daily revenue for a region over a window
    return (f"SELECT to_date(order_ts) AS d, round(sum(total_amount),2) AS revenue, count(*) AS orders "
            f"FROM {T.orders} WHERE region = '{p['region']}' "
            f"AND order_ts >= date_sub(current_date(), {p['days']}) AND status = 'COMPLETED' "
            f"GROUP BY d ORDER BY d DESC LIMIT {p['row_limit']}")


def _q09(T, p):  # order status mix over a window
    return (f"SELECT status, count(*) AS orders, round(sum(total_amount),2) AS gross "
            f"FROM {T.orders} WHERE order_ts >= date_sub(current_date(), {p['days']}) "
            f"GROUP BY status ORDER BY orders DESC LIMIT {p['row_limit']}")


def _q10(T, p):  # lifetime value by loyalty tier
    return (f"SELECT loyalty_tier, count(*) AS customers, round(avg(lifetime_value),2) AS avg_ltv "
            f"FROM {T.profiles} GROUP BY loyalty_tier ORDER BY avg_ltv DESC LIMIT {p['row_limit']}")


# Top-N --------------------------------------------------------------------

def _q11(T, p):  # best-rated menu items (min review count)
    return (f"SELECT product_id, count(*) AS reviews, round(avg(rating),3) AS avg_rating "
            f"FROM {T.reviews} GROUP BY product_id HAVING count(*) >= {p['min_reviews']} "
            f"ORDER BY avg_rating DESC, reviews DESC LIMIT {p['top_n']}")


def _q12(T, p):  # top spenders in a window
    return (f"SELECT customer_id, round(sum(total_amount),2) AS spend, count(*) AS orders "
            f"FROM {T.orders} WHERE order_ts >= date_sub(current_date(), {p['days']}) "
            f"AND status = 'COMPLETED' GROUP BY customer_id ORDER BY spend DESC LIMIT {p['top_n']}")


def _q13(T, p):  # worst-rated menu items (detractors)
    return (f"SELECT product_id, count(*) AS reviews, round(avg(rating),3) AS avg_rating "
            f"FROM {T.reviews} GROUP BY product_id HAVING count(*) >= {p['min_reviews']} "
            f"ORDER BY avg_rating ASC, reviews DESC LIMIT {p['top_n']}")


# Windowed / analytical ----------------------------------------------------

def _q14(T, p):  # 7-day rolling average revenue for a region
    return (f"SELECT d, revenue, "
            f"round(avg(revenue) OVER (ORDER BY d ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),2) AS rev_7d_avg "
            f"FROM (SELECT to_date(order_ts) AS d, sum(total_amount) AS revenue "
            f"FROM {T.orders} WHERE region = '{p['region']}' "
            f"AND order_ts >= date_sub(current_date(), {p['days']}) AND status = 'COMPLETED' "
            f"GROUP BY d) ORDER BY d DESC LIMIT {p['row_limit']}")


def _q15(T, p):  # signup cohorts by month
    return (f"SELECT date_trunc('MONTH', signup_date) AS cohort_month, count(*) AS signups "
            f"FROM {T.profiles} GROUP BY cohort_month ORDER BY cohort_month DESC LIMIT {p['row_limit']}")


# Cross-dataset insight join ----------------------------------------------

def _q16(T, p):  # churn risk: high-value customers leaving low ratings
    return (f"SELECT p.customer_id, p.lifetime_value, round(avg(r.rating),2) AS avg_rating, "
            f"count(r.review_id) AS reviews "
            f"FROM {T.profiles} p JOIN {T.reviews} r ON p.customer_id = r.customer_id "
            f"WHERE p.loyalty_tier IN ('Gold','Platinum') "
            f"GROUP BY p.customer_id, p.lifetime_value "
            f"HAVING avg(r.rating) <= {p['rating_threshold']} AND count(r.review_id) >= {p['min_reviews']} "
            f"ORDER BY p.lifetime_value DESC LIMIT {p['top_n']}")


TEMPLATES: list[Template] = [
    Template("q01_profile_lookup",       "serving",     10, _q01, ("customer_id",)),
    Template("q02_order_lookup",         "serving",      8, _q02, ("order_id",)),
    Template("q03_customer_orders",      "serving",     10, _q03, ("customer_id",)),
    Template("q04_product_reviews",      "serving",      8, _q04, ("product_id",)),
    Template("q05_customer_reviews",     "serving",      6, _q05, ("customer_id",)),
    Template("q06_customer_360",         "serving",      5, _q06, ("customer_id",)),
    Template("q07_category_avg_rating",  "aggregation",  3, _q07),
    Template("q08_region_daily_revenue", "aggregation",  3, _q08),
    Template("q09_status_mix",           "aggregation",  3, _q09),
    Template("q10_ltv_by_tier",          "aggregation",  2, _q10),
    Template("q11_top_products",         "topn",         2, _q11),
    Template("q12_top_spenders",         "topn",         2, _q12),
    Template("q13_worst_products",       "topn",         1, _q13),
    Template("q14_rolling_revenue",      "window",       1, _q14),
    Template("q15_signup_cohorts",       "window",       2, _q15),
    Template("q16_churn_risk",           "join",         1, _q16),
]

TEMPLATES_BY_ID = {t.id: t for t in TEMPLATES}

# Per-profile category multipliers let one workload skew toward serving vs analytics.
WEIGHT_MULTIPLIERS = {
    "mixed":           {"serving": 1.0, "aggregation": 1.0, "topn": 1.0, "window": 1.0, "join": 1.0},
    "serving_heavy":   {"serving": 3.0, "aggregation": 0.5, "topn": 0.5, "window": 0.3, "join": 0.3},
    "analytics_heavy": {"serving": 0.5, "aggregation": 2.0, "topn": 2.0, "window": 2.0, "join": 2.0},
}


def weighted_templates(profile: str = "mixed") -> list[tuple[Template, float]]:
    mult = WEIGHT_MULTIPLIERS[profile]
    return [(t, t.weight * mult[t.category]) for t in TEMPLATES]


class TemplatePicker:
    """Precomputed cumulative-weight sampler for fast weighted choice per query."""

    def __init__(self, profile: str = "mixed"):
        wt = weighted_templates(profile)
        self.templates = [t for t, _ in wt]
        self._cum = []
        total = 0.0
        for _, w in wt:
            total += w
            self._cum.append(total)
        self._total = total

    def pick(self, rng) -> Template:
        x = rng.random() * self._total
        return self.templates[bisect.bisect_left(self._cum, x)]


def build_sql(template: Template, tables: Tables, params: dict, run_tag: str):
    """Return (statement_text, bind_dict).

    statement_text = run-tag marker + template SQL (with ":name" markers for the
    template's high-cardinality params). bind_dict holds those bound values; it is
    empty for templates that use only literals. Passing bound params keeps the
    statement text identical across executions so the plan is compiled once."""
    text = f"{run_marker(run_tag, template.id)} {template.sql(tables, params)}"
    binds = {k: params[k] for k in template.params}
    return text, binds
