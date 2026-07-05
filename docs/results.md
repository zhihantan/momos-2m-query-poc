# Results

All figures come from the platform's own system tables (`system.query.history`,
`system.billing.usage`) filtered to a run's tag — not from the client. Regenerate
with notebook `03_analyze_results`.

Live environment: FE workspace `fe-vm-zh-serverless`, catalog
`zh_serverless_ws.momos_cx`, showcase data = **25M orders · 12M reviews · 2M
customers · 20K menu items**. Warehouse: Serverless SQL, autoscaling.

> Note on freshness: `system.query.history` ingests with a lag (~8 min observed),
> so the authoritative counts are read a few minutes after each run.

---

## Maximum run — 2,756,577 queries (distributed generator, 60-min maximize)

Run tag `momos_dist_60min`. **7 parallel `momos_benchmark_distributed` instances**
(each a `mapInPandas` generator on its own node) sharing the tag, serving mode,
against a Medium warehouse scaled to 30→40 clusters.

| metric (system.query.history) | value |
|---|---|
| **Total queries executed (proven)** | **2,756,577** |
| Within a strict 60-min window | 1,941,159 |
| Errors / not-finished | 39 (of 2.76M — 99.9986% success; ~all from job cancellation) |
| Result-cache hit rate | **99.5%** |
| Sustained aggregate QPS | **~540 QPS** (~521 over the full span) |
| Latency p50 | ~461 ms |
| Data scanned | ~0 GB (near-100% cache — no storage reads) |
| Run span | 88.2 min (the distributed wave-jobs + cancellation ran past 60 min) |
| Warehouse | Serverless SQL, Medium, autoscaled to 40 clusters |

**Key finding — a serving-throughput plateau.** Sustained QPS held at **~540**
even after scaling clusters 30→40 and generator instances 4→7. So on this
warehouse the ceiling wasn't clusters or client count — it's the per-query
**planning floor** (~270 ms, applies even to cache hits) times the per-warehouse
concurrency. To push past ~540 QPS you raise the warehouse **size** (Large/XL cut
compile+exec per query), not the cluster count. The distributed generator hit its
own single-node cap too (serverless kept each `mapInPandas` job on one node), which
is why 7 parallel instances were used. Net: 2.76M queries proven (over an 88-min
span as the wave-jobs ran long), 60-min-window 1.94M; a warehouse size-up would
clear 2M inside a strict 60 min.

## Headline — 2,000,000 queries, serving mode (single-warehouse, N driver-pool generators)

The realistic **application serving-layer** pattern: result cache on, hot-set of
popular customers/menu items, load driven by **6 generator instances** sharing one
`run_tag` (one generator node caps ~150 QPS — the client is the bottleneck, so you
scale it out). Warehouse: **Medium**, autoscale to 24 clusters.

Run tag `momos_2M_serving` (2026-07-04, workspace `fe-vm-zh-serverless`). 6
generators for the bulk, then a short 3-generator top-up — all sharing the tag, so
`system.query.history` aggregates them into one proven count.

| metric (from system.query.history) | value |
|---|---|
| **Total queries (proven, `COUNT(*)`)** | **2,007,069** |
| Client-issued total (9 registry rows) | 2,007,069 — **exactly matches** the platform count |
| Errors / not-finished | **0** |
| Result-cache hit rate (fully warmed) | **98.8%** (ramped 47% → 81% → 95% → 99.5%) |
| Sustained aggregate QPS (main run) | **~528 QPS ≈ 1.9M queries/hour** |
| Latency p50 / p95 | **356 ms / 1,687 ms** |
| Latency p99 | 37 s — warm-up cache-miss aggregations on 25M rows skew the tail; p50/p95 are representative of steady state |
| Data scanned | **594.5 GB** across the 2M queries |
| Warehouse | Serverless SQL, **Medium**, autoscaled — **peak 22 clusters** |
| Est. cost (this run) | ~$270 (Medium; ≈ half on Small — see note) |
| **≈ Cost per million queries** | ~$135 (Medium) / ~$70 (Small) — planning-floor driven |

At the sustained ~528 QPS, a single continuous run reaches 2M in ~63 min; a modest
size-up (or more clusters) brings it under 60. Here the 2M was accumulated as a
~50-min main run plus a short top-up.

Cost note: the ~270 ms per-query planning floor applies **even to cache hits**, so
serving-mode cost is dominated by planning, not execution. It is roughly
size-proportional — running the same workload on a **Small** warehouse ≈ halves the
cost for nearly the same latency (cache hits are compile-bound, not compute-bound).

---

## Companion — compute mode (result cache OFF, real Photon work)

The honest "every query does real work" measurement. `SET use_cached_result=false`,
so `from_result_cache = 0.0%` (verified). This is where you see the true per-query
cost of Serverless SQL:

| measurement (compute mode) | value |
|---|---|
| Result-cache hit rate | **0.0%** (verified — real work every query) |
| Per-query total latency (even a point lookup) | **~1.0 s** |
| ├─ compilation (fixed planning overhead) | ~270 ms |
| ├─ execution (Photon) | ~250–500 ms (aggregations higher) |
| └─ fetch / queue / overhead | ~300–450 ms |
| Point-lookup `q01` (profile by id) exec / compile / total | 258 / 266 / 959 ms |
| Sustained compute-mode QPS, Small warehouse @ 30 clusters | ~130–300 QPS |

**Implication:** at ~1 s/query, cache-off throughput is bounded by
`max_clusters × ~10 ÷ 1 s`. To push compute-mode QPS up: size up (Medium/Large cut
compile+exec) and/or raise max_clusters. To serve app-scale QPS cheaply, use the
result cache (serving mode) — which is exactly the headline above.

---

## The client is the bottleneck (key finding)

The warehouse was never the limit in these runs — the **load generator** was:

| generator config | achieved QPS (one node) |
|---|---|
| 32 threads (sf1) | ~116 |
| 320 threads (25M, compute) | ~133 |
| 120 threads (25M, serving) | ~152 |

One serverless node saturates ~130–150 QPS (Python/GIL/connection handling), so
**556+ QPS requires running several generator instances** — done here by launching
N job runs that share a `run_tag`; `system.query.history` aggregates them into a
single proven count.

## Reproduce

```bash
./scripts/setup.sh                                   # deploy + generate sf1
databricks bundle run momos_generate_data -t dev --params scale=full   # showcase data
# warm + size the warehouse (docs/tuning.md), then launch N generators sharing a run_tag:
for i in $(seq 1 6); do
  databricks api post /api/2.1/jobs/run-now --json \
   '{"job_id":<benchmark_job_id>,"notebook_params":{"mode":"serving","scale":"full",
     "pace":"false","duration_seconds":"3000","run_tag":"momos_2M_serving"}}'
done
# then: notebook 03_analyze_results (defaults to the latest run) for the proof + cost
```
