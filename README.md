# 2 Million SQL Queries in 60 Minutes — Databricks Serverless SQL

A reproducible showcase: **one Databricks Serverless SQL Warehouse serving
2,000,000 queries in ≤ 60 minutes** over realistic multi-location F&B
customer-experience data — with **zero cluster management**, and the count
**proven from Databricks' own audit log** (`system.query.history`), not the load
script.

```
2,000,000 queries ÷ 3,600 seconds = 556 queries/second, sustained for one hour.
```

This is a **concurrency + throughput** story: not "one big query is fast," but "a
serverless warehouse absorbs an application-scale query firehose, autoscaling
itself." Running it live taught us the real performance model of Serverless SQL —
which is the most useful thing here:

- **The client is the bottleneck, not the warehouse.** One generator node caps
  ~130–150 QPS (Python/connection limits), so hitting 556+ QPS means running
  **several generator instances that share a run_tag** — `system.query.history`
  aggregates them into one proven count. The warehouse had capacity to spare.
- **Two honest modes.** *Serving mode* (result cache on, hot data — the real
  app-serving pattern) hits 2M/hour cheaply. *Compute mode* (cache off, `SET
  use_cached_result=false`, **verified 0% cache**) shows the true per-query cost:
  ~1 s each (~270 ms compile + ~250–500 ms Photon exec + fetch), so cache-off
  throughput scales with clusters/size. Both are shown; see
  [docs/results.md](docs/results.md).

---

## Results (from `system.query.history`, not self-reported)

**Max run: 2,756,577 queries** served from one Serverless SQL warehouse (7 parallel
distributed generators, 99.5% cache, ~540 QPS sustained) — details and the
serving-throughput-plateau finding in **[docs/results.md](docs/results.md)**. The
two-mode picture below is from the earlier single-warehouse run:

| | Serving mode (headline) | Compute mode (real work) |
|---|---|---|
| What it shows | app serving layer at scale | true per-query cost, 0% cache |
| Queries served (proven) | **2,007,069** — 0 errors | measured per-query anatomy |
| Sustained rate | **~528 QPS ≈ 1.9M/hour** | ~130–300 QPS (planning-floor bound) |
| Result-cache hit rate | **98.8%** (warmed to 99.5%) | **0.0%** (verified) |
| Per-query latency | p50 **356 ms** | ~1 s (270 ms compile + Photon + fetch) |
| Data scanned / peak clusters | 594.5 GB / 22 | — |
| Proven by | `system.query.history COUNT(*)` | same |

The generator client count matched `system.query.history` **exactly**
(2,007,069 = 2,007,069) — the proof is airtight.

---

## Why this matters

- **Serverless, not server-full.** No clusters to size, warm, or babysit. The
  warehouse starts in seconds and autoscales 1→N clusters with load.
- **Cost per million queries** reframes the whole conversation: serving analytics
  at app scale is cheap on serverless.
- **The proof is the platform's.** Every query is tagged; we `COUNT(*)` it in
  `system.query.history` and read the dollars from `system.billing.usage`.
- **Two honest modes.** *compute* (cache-busting, real Photon work) and *serving*
  (cache-friendly, real app pattern) — you can show both truthfully.

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
| `full` (showcase) | 50K | 2M | 100M | 50M |

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
enabled, and a CLI profile (default here: `fe-vm-zh-serverless`). Adjust
`databricks.yml` (host) and `src/config/config.yaml` (catalog/schema/warehouse) for
your workspace.

```bash
# 1) Deploy the warehouse + jobs + dashboard, and generate sf1 data
./scripts/setup.sh                       # == bundle validate/deploy + generate

# 2) Validate end-to-end with a 5-minute smoke run, then read the proof
./scripts/run_benchmark.sh smoke
#   -> job runs the load generator, then 03_analyze_results prints the metrics

# 3) The headline: generate full-scale data, warm the warehouse, run 2M/60min
databricks bundle run momos_generate_data -t dev -p fe-vm-zh-serverless --params scale=full
#   set a warm floor (min clusters) — see docs/tuning.md — then:
./scripts/run_benchmark.sh full          # compute mode; add 'serving' for cache mode
```

Or open **`notebooks/00_quickstart.py`** and Run All for the whole thing at `sf1`
scale in one notebook.

**Single-job distributed generator:** the `momos_benchmark_distributed` job
(`notebooks/04_run_benchmark_distributed.py`) fans the load across Spark
**executors** via `mapInPandas` (self-contained worker — no `src` needed on
executors). It records the executor `node` per query, so `count(distinct node)`
tells you if you actually got multi-node.

```bash
databricks bundle run momos_benchmark_distributed -t dev -p fe-vm-zh-serverless
```

> **Compute matters** (measured — see [docs/tuning.md §7](docs/tuning.md)): on a
> **classic multi-node cluster** this fans across real nodes → thousands of QPS
> from one job. On a **serverless-only** workspace, serverless kept it on **one
> node** (no multi-node scale-out for this I/O-bound job), so there the reliable
> way to multi-node is the **N-parallel-jobs** pattern (several `momos_benchmark`
> runs sharing a `run_tag`) — which is how the live 2M run reached ~528 QPS.

### The dashboard

The bundle deploys an **AI/BI dashboard** ("Momos · 2M Queries") that auto-targets
the latest run: cumulative-count race to 2M, QPS timeline, latency percentiles,
the autoscaling curve, cache-hit rate, and a cost tile.

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

## Cost & cleanup

Illustrative: a **Small** warehouse averaging ~8 clusters for an hour ≈ 96 DBU ≈
**~$67** (verify your rate) → well under **$0.0001/query**. Guardrails: the
warehouse `auto_stop_mins`, a small default scale, and a client-log sample rate for
the big run.

```bash
./scripts/teardown.sh     # deletes warehouse + jobs + dashboard (stops billing)
```

## Caveats

- Pricing/DBU figures are **illustrative** — confirm for your region/contract.
- The `~10 concurrent queries per cluster` heuristic and serverless behavior can
  change; verify against current Databricks docs.
- Very high `max_num_clusters` may require an account limit increase.
- Example code, provided as-is (see [LICENSE](LICENSE)). Not officially supported.
