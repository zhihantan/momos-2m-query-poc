# Results

All figures come from Databricks' own system tables — `system.query.history` for
counts, latency, and cache; `system.billing.usage` for cost — filtered to each
run's tag, never self-reported by the client. Regenerate any time with notebook
`03_analyze_results`.

Data: a multi-location F&B star schema at showcase scale — **25M orders · 12M
reviews · 2M customers · 20K menu items** — served from Serverless SQL Warehouses.

---

## Headline — 2,007,069 queries served, proven from the audit log

The realistic application serving-layer pattern: result cache on, a hot set of
popular customers and menu items, load driven by several generator instances that
share one run tag. One Serverless SQL Warehouse:

| metric (`system.query.history`) | value |
|---|---|
| **Queries served (proven)** | **2,007,069** |
| Client's independent count | 2,007,069 — **matches the platform exactly** |
| Errors | **0** |
| Result-cache hit rate | **98.8%** (warms to 99.5%) |
| Sustained throughput | **~525 QPS ≈ ~1.9M queries/hour** (peak 30 s: **550**) |
| Latency p50 / p95 | **356 ms / 1.7 s** |
| Warehouse | Serverless SQL, autoscaling |
| Cost (measured) | **~$85 per million queries** |

**Honest timing.** At the measured ceiling this is **~1.9M queries in a clean hour;
2,000,000 takes ~63 minutes.** The number that matters is the *rate* — ~1.9M/hour
of tagged, audited, application-scale `SELECT`s from one serverless warehouse — and
that every single query is accounted for in Databricks' own audit log, matching the
generator's independent count to the query.

---

## The throughput ceiling is shared — more (or bigger) warehouses don't raise it

The most important operational finding, and a correction to an earlier assumption
in this repo. We drove the workload every way we could, and throughput
**converged on the same ~550 QPS ceiling regardless of cache mode, warehouse count,
warehouse size, or query mix.** Peak sustained 30-second rate, from
`system.query.history`:

| configuration | peak 30 s QPS |
|---|---|
| 1× Small · cache-off | 544 |
| 2× Small · cache-off | 557 |
| 3× Small · cache-off | 552 |
| 1× Medium · serving (cache on) | 550 |
| **2× Small · serving (cache on)** | **551** |

**Two warehouses serve no faster than one** (551 vs 550); three are no faster than
two. The bottleneck is **per-query compilation/planning**, which behaves as a
*shared* control-plane resource here: when multiple warehouses drive load together,
compile time balloons (**~270 ms → ~1,400 ms**) so the *total* stays pinned at
~550 QPS while each warehouse's share drops proportionally — two warehouses settle
at **~262 QPS each**. Execution time is **not** the limit; it is small (and
near-zero on cache hits). The tell is unmistakable: in the 3-warehouse run, a
warehouse running *alone* had ~500 ms compile, while two warehouses *overlapping*
had ~1,400 ms each — same hardware, different contention.

> **Environment caveat — read before quoting this.** This shared ceiling is almost
> certainly a characteristic of **this specific (FE-VM demo) workspace's shared
> control plane, not a fundamental Serverless SQL limit.** On a production
> workspace/account with full control-plane capacity, independent warehouses are
> expected to scale independently. **Re-verify on your target workspace** with the
> method below — run one warehouse, then two sharing a run tag, and compare the
> peak-30s QPS and the per-warehouse `compilation_duration_ms`. If the second
> warehouse adds throughput and compile stays flat, your workspace scales out; if
> the total stays flat and compile balloons, you've hit the same shared ceiling.

---

## Cache on vs off — same ceiling, very different economics

Because throughput is compile-bound, **the result cache does not raise the
throughput ceiling** — both modes top out at ~550 QPS. What the cache changes is
**cost and latency**: it makes execution nearly free, so you reach the ceiling with
a fraction of the compute.

| | **Serving (cache on)** | **Cache off** |
|---|---|---|
| Result-cache hit rate | 94–99% | **0%** |
| Execution per query | **~3–33 ms** (near-free) | 122–240 ms (real scan) |
| Latency p50 | **356 ms** | 678 ms → ~1 s |
| Peak throughput | ~550 QPS | ~550 QPS — **the same** |
| Compute to reach the ceiling | **one warehouse (~20 clusters)** | **two warehouses (~80 clusters)** |
| Cost per 2M queries | **~$85** | **~$500 (~6×)** |

With the cache **off**, every query does real work — measured ~1 s end-to-end
(~300 ms compile + ~240 ms execution + fetch/queue) — so a single Small warehouse
is *cluster*-limited to **~407 QPS** and you need a **second** warehouse just to
reach the same ~550 ceiling the cache hits on one. You pay for ~4× the clusters
**and** for real execution on every query: roughly **6× the cost for identical
throughput and worse latency.**

**This is the case for the cache-backed serving pattern.** It does not make the
warehouse faster at its ceiling — it makes serving ~1.9M queries/hour *cheap and
low-latency* instead of expensive and slow.

> Turn the cache off only when queries are genuinely unique, or the underlying data
> changes on nearly every request (the cache can't help either way), or to
> benchmark raw engine throughput. For a read-heavy serving layer hitting popular
> data — the Momos pattern — keep it on.

---

## Cost (measured from `system.billing.usage`)

Costs are read from `system.billing.usage` at the **$0.70/DBU** list rate (verify
your contract/region). Serving-mode throughput is compile-bound and
size-independent, so for a **cache-served** workload cost scales purely with the
per-cluster DBU rate — making **Small the cost-optimal size**:

| size (serving) | ≈ $ / 1M queries | ≈ $ / 2M queries |
|---|---|---|
| **Small** (recommended) | **~$42** | **~$85** |
| Medium (measured) | ~$85 | ~$170 |
| Large | ~$140 | ~$285 |

**Cache-off costs ~6× more for the same throughput:** reaching the ~550 QPS ceiling
with the cache off took **two** Small warehouses at ~40 clusters each (~80 clusters,
mostly compile-starved) plus real execution on every query — on the order of
**~$500 per 2M queries** versus ~$85 served from cache.

---

## Peak run — 2,756,577 queries from one warehouse

Pushing a single warehouse as hard as possible (serving mode, several distributed
generator instances) served **2,756,577** queries at 99.5% cache — but the
*sustained rate held at ~525 QPS*, the same ceiling, over a longer window. You can
serve an arbitrarily large *total* by running longer; you cannot exceed the ~550 QPS
*rate* by adding warehouses.

---

## Scaling the load generator (to reach the ceiling)

The warehouse ceiling only binds once the *client* can push that hard. One
generator node tops out around **~130–150 QPS** (Python/connection overhead):

| generator (one node) | achieved QPS |
|---|---|
| 32 threads | ~116 |
| 120 threads | ~152 |
| 320 threads | ~133 |

So reaching ~550 QPS needs **~4–5 generator instances sharing one run tag** —
`system.query.history` aggregates them into a single proven count. Beyond ~5
generators you stop gaining, because the shared **warehouse-side** compile ceiling
(above) now binds, not the client. (Note: launching many generator jobs at once can
stagger — this workspace's serverless job-compute pool spun up ~10 at a time — which
also drags real wall-clock throughput on the biggest runs.)

---

## Reproduce

```bash
./scripts/setup.sh                                   # deploy + generate sf1 data
databricks bundle run momos_generate_data --params scale=full   # showcase-scale data
# warm/size the warehouse (see docs/tuning.md), then launch N generators sharing a run tag:
for i in $(seq 1 5); do
  databricks api post /api/2.1/jobs/run-now --json \
   '{"job_id":<benchmark_job_id>,"notebook_params":{"mode":"serving","scale":"full",
     "template_profile":"serving_heavy","pace":"false","duration_seconds":"3600",
     "run_tag":"momos_serving_run"}}'
done
# then run notebook 03_analyze_results (defaults to the latest run) for the proof + cost
```

To reproduce the **cache-off** comparison, launch the same generators with
`"mode":"compute"` (sets `use_cached_result = false`); to test **cross-warehouse
scaling** on your workspace, point half the generators at a second warehouse and
compare peak-30s QPS and per-warehouse compile time (see the caveat above).
