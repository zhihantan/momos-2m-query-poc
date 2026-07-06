# 2 Million SQL Queries in an Hour — Databricks Serverless SQL

A reproducible showcase: **a Databricks Serverless SQL Warehouse serving
application-scale query volume — 2,007,069 queries proven from Databricks' own
audit log** (`system.query.history`, not the load script) — over realistic
multi-location F&B customer-experience data, with **zero cluster management**.

```
Measured: ~525 QPS sustained (peak 550) ≈ ~1.9M queries/hour.
2,000,000 queries land in ~63 minutes — the sustained rate is the story, not one big query.
```

This is a **concurrency + throughput** story: not "one big query is fast," but "a
serverless warehouse absorbs an application-scale query firehose, autoscaling
itself." Running it live taught us the real performance model of Serverless SQL —
the most useful thing here:

- **Throughput is compile-bound at a shared ~550 QPS ceiling** on this workspace,
  and that ceiling does **not** rise with more warehouses, a bigger size, or the
  cache (all measured — see [docs/results.md](docs/results.md)). This is very
  likely a limit of *this shared demo workspace's control plane*, not of Serverless
  SQL in general — **re-verify on your own workspace** (method in the results doc).
- **The cache pays off in cost and latency, not throughput.** Cache-on and
  cache-off hit the *same* ~550 QPS ceiling, but cache-on serves ~1.9M/hour at
  **~$85/million and 356 ms p50**, while cache-off costs **~6× more** and runs ~3×
  slower (real execution on every query). This is the cache-backed serving pattern
  a real app drives.
- **The client is usually the first bottleneck.** One generator node caps
  ~130–150 QPS, so reaching ~550 QPS means **~4–5 generator instances sharing a
  run_tag** — `system.query.history` aggregates them into one proven count.

---

## Results (from `system.query.history`, not self-reported)

**Largest run: 2,756,577 queries** served from one Serverless SQL warehouse
(99.5% cache), holding the same ~525 QPS ceiling over a longer window — full detail
in **[docs/results.md](docs/results.md)**. The headline serving run:

| metric | value |
|---|---|
| Queries served (proven) | **2,007,069** — 0 errors |
| Sustained rate | **~525 QPS ≈ ~1.9M/hour** (peak 30 s: 550) |
| Result-cache hit rate | **98.8%** (warms to 99.5%) |
| Latency p50 / p95 | **356 ms / 1.7 s** |
| Cost (measured) | **~$85 / million** |
| Proven by | `system.query.history COUNT(*)` |

The generator's client count matched `system.query.history` **exactly**
(2,007,069 = 2,007,069) — the proof is airtight.

### Best warehouse configuration

For this cache-backed serving workload, the cost-optimal Serverless SQL Warehouse:

| setting | value | why |
|---|---|---|
| **Size** | **Small** | throughput is compile-bound and **size-independent** (same ~550 QPS ceiling on Small/Medium/Large) — a bigger size costs 2–3× for the same rate |
| **Type** | Serverless + Photon | instant start, autoscaling, result cache |
| **Clusters (serving)** | min 1 → max ~20 | ~10 concurrent queries per cluster; ~20 comfortably covers the ~550 QPS ceiling on cache hits |
| **Clusters (timed run)** | warm floor min 4–6 | avoids the cold-start ramp eating into the window |
| **Auto-stop** | 10 min | ~$0 when idle |

One Small warehouse serves **~1.9M queries/hour at ~$85/million** and reaches the
workspace's ~550 QPS ceiling on its own. **Adding warehouses does not raise the
rate** — we measured one, two, and three warehouses (both cache-on and cache-off)
all pinned at ~550 QPS, because compilation is a *shared* control-plane resource on
this workspace (see [docs/results.md](docs/results.md#the-throughput-ceiling-is-shared--more-or-bigger-warehouses-dont-raise-it)).
So the recommended config is **one Small warehouse**; 2M lands in ~63 min. This
shared ceiling is likely specific to this demo workspace — **re-verify on yours.**

---

## Why this matters

- **Serverless, not server-full.** No clusters to size, warm, or babysit. The
  warehouse starts in seconds and autoscales 1→N clusters with load.
- **Cost per million queries** reframes the whole conversation: serving analytics
  at app scale is cheap on serverless.
- **The proof is the platform's.** Every query is tagged; we `COUNT(*)` it in
  `system.query.history` and read the dollars from `system.billing.usage`.
- **Cache-backed serving layer.** Popular customers and menu items are served from
  the result cache at app scale — the pattern a real front end drives.

---

## The data (multi-location F&B customer experience)

Star schema in `zh_serverless_ws.momos_cx`, generated with distributed Spark:

- **customer_profiles** — loyalty tier, lifetime value, region, channel
- **customer_orders** — amounts, tips, status, store, channel (power-law customer activity)
- **customer_reviews** — rating, sentiment, helpful votes (correlated), verified purchase
- **products** — menu items (category, brand, price)

Referential integrity is built in; tables are **liquid-clustered** on
`customer_id` / `product_id` so lookups and joins prune. Two scales:

| profile | products | profiles | orders | reviews |
|---|---|---|---|---|
| `sf1` (default) | 1K | 100K | 1M | 500K |
| `full` (showcase) | 20K | 2M | 25M | 12M |

## The workload

16 parameterized templates mapping to real F&B questions, spanning the latency
spectrum: point lookups (profile / order / reviews), customer-360, aggregations
(revenue by region, rating by category, LTV by tier), top-N (best/worst menu
items, top spenders), windowed (7-day rolling revenue, signup cohorts), and a
cross-dataset churn-risk join. Every template is `LIMIT`-bounded and carries a
run-tag comment.

---

## Run it

**Prerequisites:** Databricks CLI ≥ 0.240, a workspace with **serverless SQL**
enabled, and a CLI profile (default here: `your-profile`). Adjust
`databricks.yml` (host) and `src/config/config.yaml` (catalog/schema/warehouse) for
your workspace.

```bash
# 1) Deploy the warehouse + jobs + dashboard, and generate sf1 data
./scripts/setup.sh                       # == bundle validate/deploy + generate

# 2) Validate end-to-end with a 5-minute smoke run, then read the proof
./scripts/run_benchmark.sh smoke
#   -> job runs the load generator, then 03_analyze_results prints the metrics

# 3) The headline: generate full-scale data, warm the warehouse, run 2M/60min
databricks bundle run momos_generate_data -t dev -p your-profile --params scale=full
#   set a warm floor (min clusters) — see docs/tuning.md — then:
./scripts/run_benchmark.sh full serving  # serving mode (result cache)
```

Or open **`notebooks/00_quickstart.py`** and Run All for the whole thing at `sf1`
scale in one notebook.

**Single-job distributed generator:** the `momos_benchmark_distributed` job
(`notebooks/04_run_benchmark_distributed.py`) fans the load across Spark
**executors** via `mapInPandas` (self-contained worker — no `src` needed on
executors). It records the executor `node` per query, so `count(distinct node)`
tells you if you actually got multi-node.

```bash
databricks bundle run momos_benchmark_distributed -t dev -p your-profile
```

> **Cluster type matters** (measured — see [docs/tuning.md §7](docs/tuning.md)): on a
> **classic multi-node cluster** this fans across real nodes → thousands of QPS
> from one job. On a **serverless-only** workspace, serverless kept it on **one
> node** (no multi-node scale-out for this I/O-bound job), so there the reliable
> way to multi-node is the **N-parallel-jobs** pattern (several `momos_benchmark`
> runs sharing a `run_tag`) — which is how the live 2M run reached ~528 QPS.

### The dashboard

The bundle deploys an **AI/BI dashboard** ("Momos · 2M Queries") that auto-targets
the latest run: cumulative-count race to 2M, QPS timeline, latency percentiles,
the autoscaling curve, and cache-hit rate.

---

## How the proof works

Every query is prefixed with `/* <run_tag> tpl=<id> */`. That comment is preserved
in `system.query.history.statement_text`, so the analysis filters the audit log to
exactly one run:

```sql
SELECT count(*) AS proven_total
FROM system.query.history
WHERE statement_text LIKE '/* <run_tag> %';   -- analysis queries start with SELECT, so never self-count
```

Latency percentiles, `from_result_cache` hit rate, bytes read, and file pruning all
come from the same table. Actual cost comes from `system.billing.usage` attributed
to the warehouse (`usage_metadata.warehouse_id`) — note it can lag a few hours.

---

## Repository layout

```
databricks.yml            Asset Bundle root
resources/                warehouse.yml · jobs.yml · dashboard.yml
src/
  config/config.yaml      single source of truth (scale, warehouse, benchmark knobs)
  common/config.py        config loading + run-tag marker
  data_generation/        Spark synthesis of the star schema
  workload/               query_templates · param_sampler · load_generator · distributed
  analysis/               metrics (query.history) · cost (billing.usage)
notebooks/                00_quickstart · 01_generate · 02_benchmark · 03_analyze · 04_benchmark_distributed
dashboards/               query_throughput.lvdash.json
docs/                     architecture · tuning · results
scripts/                  setup · run_benchmark · teardown
```

## Cost (measured from `system.billing.usage`)

Actual DBU consumption billed to the warehouse — **not estimates** — at the list
serverless-SQL rate of **$0.70/DBU** (verify your contract/region).

**Headline 2M serving run** (Medium warehouse, cache-served): **≈ $170**, or
**≈ $85 per million queries**.

Serving-mode throughput is *size-independent* (all sizes cap at the shared ~550 QPS
ceiling — see [docs/results.md](docs/results.md)), so cost scales with the
per-cluster DBU rate. That makes **Small the cost-optimal choice** — same throughput,
lowest price:

| warehouse size | ≈ $ / 1M queries | ≈ $ / 2M run |
|---|---|---|
| **Small** (recommended) | **~$42** | **~$85** |
| Medium (measured) | ~$85 | ~$170 |
| Large | ~$140 | ~$285 |

**Turning the cache off costs ~6× more for the same throughput.** Reaching the same
~550 QPS ceiling with the result cache off took **two** Small warehouses (~80
clusters, mostly compile-starved) plus real execution on every query — on the order
of **~$500 per 2M** versus ~$85 served from cache. Likewise, pushing one warehouse
to 40 clusters for peak throughput is over-provisioned for cache hits (execution is
nearly free). For a cache-served serving layer, a **Small** warehouse at the ~550
QPS ceiling moves queries cheapest.

## Cleanup

Guardrails: warehouse `auto_stop_mins`, a small default scale, and a client-log
sample rate for large runs.

```bash
./scripts/teardown.sh     # deletes warehouse + jobs + dashboard (stops billing)
```

## Caveats

- Costs are **measured** from `system.billing.usage` at the **$0.70/DBU** list rate —
  confirm your contract/region rate.
- The `~10 concurrent queries per cluster` heuristic and serverless behavior can
  change; verify against current Databricks docs.
- Very high `max_num_clusters` may require an account limit increase.
- Example code, provided as-is (see [LICENSE](LICENSE)). Not officially supported.
