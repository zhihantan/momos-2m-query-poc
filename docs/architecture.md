# Architecture

## The claim, stated precisely

> A single Databricks **Serverless SQL Warehouse** serves an application-scale
> workload — **~1.9M SQL queries per hour** (measured ~525 QPS sustained, ~550 QPS
> peak; **2,007,069** proven from the audit log, landing in ~63 minutes) — over
> realistic customer-experience data, with no cluster management, at sub-second
> latency, for a cost on the order of **$0.0001 per query** — and the platform's own
> system tables prove it.

This is a concurrency / throughput benchmark, not a "one big query is fast"
benchmark. **The throughput ceiling is ~550 QPS and is shared at the workspace
level** — it does not rise with more/bigger warehouses or the result cache (see
[results.md](results.md)); on this (demo) workspace that caps a run at ~1.9M/hour,
likely a shared control-plane limit rather than a fundamental Serverless SQL one.

## Components

```
 Data-Gen Job (serverless Spark)                 Serverless SQL Warehouse
   spark.range + expressions        ─ reads ▶     momos-benchmark-wh
   ├─ products (menu items)                        Small · autoscale 1..20 clusters
   ├─ customer_profiles                            Photon · result + disk cache
   ├─ customer_orders          writes             ▲
   └─ customer_reviews  ──────────────┐            │ ~550 QPS of tagged SELECTs
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

## The workload — a serving-layer pattern

The parameter sampler draws IDs from a small fixed "hot set" (like a real app or
dashboard hitting popular customers/menu items), with the result cache on. Repeated
access to popular data is served from cache at ultra-low latency and near-zero
marginal cost — the realistic serving-layer pattern.

## Why the client is scaled out

The *client* is usually the first bottleneck in a query benchmark. Every query here
is a network round-trip, so the threads spend nearly all their time blocked on the
warehouse (releasing the GIL). But a single generator node still tops out at
**~130–150 QPS** (Python/connection overhead), so to reach the ceiling we run
**several generator instances that share one run_tag** — `system.query.history`
aggregates them into one proven count. An executor-distributed variant
(`src/workload/distributed.py`, via `mapInPandas`) does the same fan-out inside one
job on a multi-node cluster; see [tuning.md](tuning.md).

Past **~4–5 generators the bottleneck flips to the warehouse side** — the shared
~550 QPS compilation ceiling (see the claim above and [results.md](results.md)) —
so adding more client capacity, more warehouses, or a bigger size no longer raises
the rate. On this workspace the client and warehouse ceilings happen to land near
each other (~550 QPS).

The load generator runs on **separate compute** from the warehouse, so we measure
the warehouse, not the generator.

## Why system tables are the credibility anchor

Every benchmark query is prefixed with `/* <run_tag> tpl=<id> */`. That comment
survives verbatim in `system.query.history.statement_text`, so we can filter the
platform's own audit log to exactly one run and `COUNT(*)` it. The headline
"2,000,000" is Databricks' number, not the script's. Cost comes the same way,
from `system.billing.usage`.
