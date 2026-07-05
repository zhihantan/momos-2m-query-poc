# Architecture

## The claim, stated precisely

> A single Databricks **Serverless SQL Warehouse** can serve an application-scale
> workload — **2,000,000 SQL queries in ≤ 60 minutes** (~556 queries/second) —
> over realistic customer-experience data, with no cluster management, at
> single-digit-millisecond-to-sub-second latency, for a cost on the order of
> **$0.0001 per query** — and the platform's own system tables prove it.

2,000,000 ÷ 3,600 s = **556 QPS sustained for an hour.** This is a concurrency /
throughput benchmark, not a "one big query is fast" benchmark.

## Components

```
 Data-Gen Job (serverless Spark)                 Serverless SQL Warehouse
   spark.range + expressions        ─ reads ▶     momos-benchmark-wh
   ├─ products (menu items)                        Small · autoscale 1..20 clusters
   ├─ customer_profiles                            Photon · result + disk cache
   ├─ customer_orders          writes             ▲
   └─ customer_reviews  ──────────────┐            │ ~556 QPS of tagged SELECTs
     (Delta, liquid-clustered)         ▼           │
                              <catalog>.<schema>   │
                                                   │
 Load Generator (serverless, driver thread pool)   │
   N threads, each: 1 connection, paced loop ──────┘
   every query carries  /* <run_tag> tpl=<id> */
   client-observed latency  ─ writes ▶  benchmark_query_log (Delta)
                                        benchmark_runs (Delta)

 Proof / Analysis                     AI/BI Dashboard
   system.query.history  ── the authoritative count + latency + cache-hit
   system.billing.usage  ── actual DBUs -> $ and $/million queries
   system.compute.warehouse_events ── the autoscaling curve
```

## Data model (F&B / multi-location customer experience)

Star schema in `<catalog>.<schema>` (default `zh_serverless_ws.momos_cx`):

| table | grain | key columns |
|---|---|---|
| `products` | menu item | product_id, category, brand, base_price |
| `customer_profiles` | customer | customer_id, loyalty_tier, lifetime_value, region, channel |
| `customer_orders` | order | order_id, customer_id, order_ts, status, total_amount, tip, store_id |
| `customer_reviews` | review | review_id, customer_id, order_id, product_id, rating, sentiment |

Referential integrity is built in; customer activity follows a power law (a
minority of customers drive most orders/reviews); ratings correlate with
sentiment and helpful-vote counts. Tables are **liquid-clustered** on the
join/lookup keys (`customer_id`, `product_id`) so point lookups and joins prune.

## Two honest workload modes

The parameter sampler's key distribution controls the cache — so you can tell two
truthful stories:

- **compute** — IDs drawn uniformly across the full key space; the client also
  issues `SET use_cached_result = false`. The warehouse does real Photon work for
  ~556 QPS. This is the honest compute stress test.
- **serving** — IDs drawn from a small fixed "hot set" (like a real app or
  dashboard hitting popular customers/menu items). The result + disk caches are
  exploited: ultra-low latency, near-zero marginal cost.

## Why the client is scaled out

The bottleneck in a query benchmark is almost always the *client*, not the
warehouse. Every query here is a network round-trip, so the threads spend nearly
all their time blocked on the warehouse (releasing the GIL). But a single
generator node still tops out at **~130–150 QPS** (Python/connection overhead), so
to reach 556+ QPS we run **several generator instances that share one run_tag** —
`system.query.history` aggregates them into one proven count. An
executor-distributed variant (`src/workload/distributed.py`, via `mapInPandas`)
does the same fan-out inside one job on a multi-node cluster; see
[tuning.md](tuning.md).

The load generator runs on **separate compute** from the warehouse, so we measure
the warehouse, not the generator.

## Why system tables are the credibility anchor

Every benchmark query is prefixed with `/* <run_tag> tpl=<id> */`. That comment
survives verbatim in `system.query.history.statement_text`, so we can filter the
platform's own audit log to exactly one run and `COUNT(*)` it. The headline
"2,000,000" is Databricks' number, not the script's. Cost comes the same way,
from `system.billing.usage`.
