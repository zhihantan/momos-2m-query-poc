"""Parameter sampling for query templates.

The sampler's key-distribution *is* the cache lever:

  compute mode  -> IDs drawn uniformly across the FULL key space. Repeats are
                   rare, so the result cache mostly misses and the warehouse does
                   real Photon work. (The load generator additionally issues
                   ``SET use_cached_result = false`` in this mode.)

  serving  mode -> IDs drawn from a small, fixed "hot set" (like a real app or
                   dashboard hitting the same popular customers/menu items). The
                   same query text recurs, so the result + disk caches are
                   exploited: ultra-low latency, near-zero marginal cost.

The hot set is built from a FIXED seed so every worker/thread shares the same hot
keys — that alignment is what produces cache hits across the fleet.
"""
from __future__ import annotations

import random

REGIONS = ["APAC", "AMER", "EMEA"]
DAYS_CHOICES = [7, 14, 30, 90]
MIN_REVIEWS_CHOICES = [5, 10, 20]
TOP_N_CHOICES = [10, 20, 50]
RATING_THRESHOLDS = [2, 3]

_HOT_SEED = 20260704  # fixed so the hot set is identical across all workers


class ParamSampler:
    def __init__(self, cfg: dict, scale: dict, mode: str, seed: int | None = None):
        self.mode = mode
        self.n_customers = int(scale["profiles"])
        self.n_products = int(scale["products"])
        self.n_orders = int(scale["orders"])
        self.row_limit = int(cfg["benchmark"]["result_row_limit"])
        hot = int(cfg["benchmark"]["hot_set_size"])

        # Per-worker RNG for *selection*; shared fixed RNG for the hot *pools*.
        self.rng = random.Random(seed)
        hot_rng = random.Random(_HOT_SEED)
        self.hot_customers = [hot_rng.randrange(self.n_customers) for _ in range(hot)]
        self.hot_products = [hot_rng.randrange(self.n_products) for _ in range(hot)]
        self.hot_orders = [hot_rng.randrange(self.n_orders) for _ in range(hot)]
        # a small hot slice of the low-cardinality params (serving mode)
        self.hot_region = REGIONS[0]
        self.hot_days = 30

    # --- key sampling: uniform (compute) vs hot-set (serving) ---
    def _customer_id(self):
        return (self.rng.choice(self.hot_customers) if self.mode == "serving"
                else self.rng.randrange(self.n_customers))

    def _product_id(self):
        return (self.rng.choice(self.hot_products) if self.mode == "serving"
                else self.rng.randrange(self.n_products))

    def _order_id(self):
        return (self.rng.choice(self.hot_orders) if self.mode == "serving"
                else self.rng.randrange(self.n_orders))

    def _region(self):
        return self.hot_region if self.mode == "serving" else self.rng.choice(REGIONS)

    def _days(self):
        return self.hot_days if self.mode == "serving" else self.rng.choice(DAYS_CHOICES)

    def sample(self) -> dict:
        """A superset of every parameter any template might reference. Cheap to
        compute all of them; each template picks what it needs."""
        return {
            "customer_id": self._customer_id(),
            "order_id": self._order_id(),
            "product_id": self._product_id(),
            "region": self._region(),
            "days": self._days(),
            "min_reviews": self.rng.choice(MIN_REVIEWS_CHOICES),
            "top_n": self.rng.choice(TOP_N_CHOICES),
            "rating_threshold": self.rng.choice(RATING_THRESHOLDS),
            "row_limit": self.row_limit,
        }
