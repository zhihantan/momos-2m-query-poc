# Results

All figures come from Databricks' own system tables — `system.query.history` for
counts, latency, and cache; `system.billing.usage` for cost — filtered to each
run's tag, not self-reported by the client. Regenerate any time with notebook
`03_analyze_results`.

Data: a multi-location F&B star schema at showcase scale — **25M orders · 12M
reviews · 2M customers · 20K menu items** — served from a single Serverless SQL
Warehouse.

---

## Headline — 2,000,000 queries served (serving mode)

The realistic application serving-layer pattern: result cache on, a hot set of
popular customers and menu items, load driven by several generator instances that
share one run tag. One Serverless SQL Warehouse (Medium, autoscaling):

| metric (`system.query.history`) | value |
|---|---|
| **Queries served (proven)** | **2,007,069** |
| Client's independent count | 2,007,069 — **matches the platform exactly** |
| Errors | **0** |
| Result-cache hit rate | **98.8%** (warms to 99.5%) |
| Sustained throughput | **~528 QPS ≈ 1.9M queries/hour** |
| Latency p50 / p95 | **356 ms / 1.7 s** |
| Warehouse | Serverless SQL, Medium (peak 22 clusters) |
| Cost (measured) | **~$170** → **~$85 per million queries** |

Every query is accounted for in Databricks' audit log, and the generator's
independent count matched it to the query — the proof is airtight.

---

## Cost / performance — the cheapest config

**Serving-mode throughput is compile-bound at ~520–550 QPS per warehouse, and that
ceiling is independent of warehouse size and cluster count.** Measured across three
sizes (serving mode, cache-served):

| warehouse | sustained QPS | avg compile | avg execution |
|---|---|---|---|
| **Small** | ~520 | ~420 ms | ~5 ms |
| Medium | ~552 | ~610 ms | ~3 ms |
| Large | ~490–545 | ~360 ms | ~4 ms |

Scaling clusters 20→40 moved throughput only 528→552, and adding client capacity
didn't help either — the limit is the **per-query planning/compile overhead**
(applied even to cache hits), while cache-hit **execution is essentially free
(~3–5 ms)**.

**So don't pay for a bigger warehouse.** Because execution is free and size doesn't
raise the ceiling, cost scales purely with the per-cluster DBU rate — making
**Small the cost-optimal choice**. Costs are measured from `system.billing.usage`
at the $0.70/DBU list rate:

| size | ≈ $ / 1M queries | ≈ $ / 2M queries |
|---|---|---|
| **Small** (recommended) | **~$42** | **~$85** |
| Medium (measured) | ~$85 | ~$170 |
| Large | ~$140 | ~$285 |

A single **Small** warehouse serves **~1.9M queries/hour for ~$85**. To serve 2M+
within the hour, run **two Small warehouses in parallel** (~1,100 QPS) — still far
cheaper per query than one larger warehouse.

> If your workload is **compute mode** (cache off, real scans) rather than
> cache-served, execution is no longer free and a bigger size *does* help.

---

## Peak throughput — 2,756,577 queries from one warehouse

Driving the same warehouse harder with several distributed generator instances
(serving mode, Medium autoscaled to 40 clusters):

| metric (`system.query.history`) | value |
|---|---|
| **Queries served (proven)** | **2,756,577** |
| Result-cache hit rate | **99.5%** |
| Sustained throughput | **~540 QPS** |
| Latency p50 | ~460 ms |
| Data scanned | ~0 GB (near-100% cache) |

Throughput holds at ~540 QPS even at 40 clusters — the same per-warehouse compile
ceiling. To go beyond it, add warehouses; to serve most cheaply, size down (see the
cost section above).

---

## Companion — compute mode (result cache OFF)

The honest "every query does real work" measurement (`SET use_cached_result =
false`; verified `from_result_cache = 0.0%`). This shows the true per-query cost of
Serverless SQL:

| measurement (compute mode) | value |
|---|---|
| Result-cache hit rate | **0.0%** (real work every query) |
| Per-query latency (even a point lookup) | **~1.0 s** |
| ├─ compilation | ~270 ms |
| ├─ execution (Photon) | ~250–500 ms |
| └─ fetch / queue | ~300–450 ms |
| Sustained QPS (Small, 30 clusters) | ~130–300 |

At ~1 s/query, cache-off throughput is bounded by `clusters × ~10 ÷ 1 s`, so it
scales with size and cluster count — the opposite of the cache-served ceiling. For
app-scale QPS at low cost, serve from the result cache (the headline above).

---

## Scaling the load generator

The warehouse is rarely the first bottleneck — the load generator is. One
generator node tops out around ~130–150 QPS (Python/connection overhead):

| generator (one node) | achieved QPS |
|---|---|
| 32 threads | ~116 |
| 120 threads (serving) | ~152 |
| 320 threads (compute) | ~133 |

Higher aggregate QPS therefore comes from running **several generator instances
that share one run tag** — `system.query.history` aggregates them into a single
proven count. In production the "generator" is your application fleet, already
spread across many machines, so this is a benchmarking concern, not a platform
limit.

---

## Reproduce

```bash
./scripts/setup.sh                                   # deploy + generate sf1 data
databricks bundle run momos_generate_data --params scale=full   # showcase-scale data
# warm/size the warehouse (see docs/tuning.md), then launch N generators sharing a run tag:
for i in $(seq 1 6); do
  databricks api post /api/2.1/jobs/run-now --json \
   '{"job_id":<benchmark_job_id>,"notebook_params":{"mode":"serving","scale":"full",
     "pace":"false","duration_seconds":"3000","run_tag":"momos_serving_run"}}'
done
# then run notebook 03_analyze_results (defaults to the latest run) for the proof + cost
```
