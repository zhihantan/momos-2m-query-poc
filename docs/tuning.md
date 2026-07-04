# Tuning guide — how to actually hit 556 QPS (and go beyond)

## 1. Scale OUT, not UP

For **many small queries**, adding *clusters* beats a bigger *size*.

- Databricks targets roughly **~10 concurrent queries per cluster** before it
  queues and scales out. So a warehouse's concurrency ceiling ≈
  `max_num_clusters × 10`.
- A **Small** warehouse with `max_num_clusters: 20` gives a ~200 concurrent-query
  ceiling — plenty for 556 QPS of light queries.
- A bigger *size* (Medium/Large) makes each individual query faster (more
  parallelism per query) but costs more per cluster-hour. Use it only if the
  workload has heavy analytical queries that are individually slow.

**Sizing math.** With light serving queries at p50 ≈ 150 ms, one cluster does
`~10 / 0.15 ≈ 66 QPS`. For 556 QPS you need `~9` clusters; configure `max=20` for
headroom and variance.

## 2. Use a warm floor for the timed run

A cold warehouse starts at `min_num_clusters` and autoscales up as load arrives —
but the ramp (a few seconds per added cluster) eats into your 60 minutes. For the
**timed 2M run**, set a warm floor so steady state is reached almost immediately:

```bash
WID=$(databricks warehouses list -p PROFILE -o json \
      | python3 -c 'import sys,json;print([w["id"] for w in json.load(sys.stdin) if w["name"]=="momos-benchmark-wh"][0])')
databricks warehouses edit "$WID" --min-num-clusters 6 --max-num-clusters 20 \
  --cluster-size Small --enable-serverless-compute -p PROFILE
```

For the **"watch it autoscale from cold"** story, do the opposite: `min=1`, then
chart `system.compute.warehouse_events.cluster_count` over the run.

## 3. Concurrency on the client

In-flight concurrency ≈ `num_partitions × threads_per_partition`. Keep it near the
warehouse ceiling (~150–200). Too little and you can't reach target QPS; too much
and queries queue (watch `waiting_at_capacity_duration_ms`).

- `pace: true` targets a fixed QPS (for the "2M in 60 min" headline).
- `pace: false` is "unleashed" — finds the maximum sustainable QPS.

## 4. Cache modes

- **compute** mode issues `SET use_cached_result = false` and draws IDs uniformly,
  so the engine does real work. This is what proves throughput.
- **serving** mode leaves the result cache on and draws from a hot set; expect a
  high `from_result_cache` rate, far lower latency, and far lower cost. This is the
  realistic app/dashboard pattern.

Report `cache_hit_rate` transparently either way.

## 5. Data layout

Tables are **liquid-clustered** on `customer_id` / `product_id`. This is what makes
point lookups and joins prune files instead of scanning. Check pruning in the
results: `pruned_files / (pruned_files + read_files)`. Keep result sets tiny
(every template has a `LIMIT`) so you measure the engine, not result transfer.

## 6. Watch the client-side ceiling & rate limits

- If achieved QPS plateaus while the warehouse is *not* queuing (`waiting_*` low)
  and cache hit rate is as expected, the **client** is the bottleneck — add
  threads or nodes.
- The **SQL connector (Thrift)** used here avoids the Statement Execution
  **REST API** rate limits, which matters at hundreds of QPS. If you switch to the
  REST API, check per-workspace limits first.

## 7. Going beyond one node — the distributed generator (implemented)

`src/workload/distributed.py` + notebook `04_run_benchmark_distributed.py` (job
`momos_benchmark_distributed`) fan the per-thread loop across Spark **executors**
via `mapInPandas`, so **one job** scales past the ~150 QPS single-node cap.

Two things make it work:
- **The worker is self-contained.** Executor Python workers don't have `src` on
  their path, so the driver pre-renders the whole workload into plain picklable
  data (concrete SQL + which params are bound) and captures it in the worker
  closure. The worker imports only `databricks.sql` (a `%pip` package that *is*
  available to UDF workers) and the stdlib — never `src`. This part is verified
  on serverless (connector imports and queries fine from executor workers).
- **Concurrency = `num_partitions × threads_per_partition`,** spread across
  whatever executors the compute provides.

### ⚠️ Measured caveat on serverless-only workspaces

On this FE-VM (serverless-only) workspace, serverless **did not scale
`mapInPandas` across multiple nodes** for this I/O-bound job — it kept everything
on **one node** (`count(distinct node) = 1`) and ran the partitions in *waves*
(96 partitions × 180s ran in ~828s of wall time). Net: on serverless-only,
`mapInPandas` maxes a single node (~220 QPS — a bit above the driver pool's ~150,
since it uses more cores), it does **not** give multi-node scale-out.

So pick your generator by compute type:
- **Classic multi-node cluster** (most customer workspaces): this same code fans
  the thread pools across real executor nodes → thousands of QPS from one job.
  Keep `num_partitions` ≫ total cores.
- **Serverless-only** (this workspace): use the **N-parallel-jobs** pattern
  instead — launch several `momos_benchmark` runs sharing one `run_tag`; each gets
  its own node, and `system.query.history` aggregates them (this is how the live
  2M run hit ~528 QPS). For the distributed job here, set `num_partitions` ≈ one
  node's cores so it runs in a single wave rather than queuing.

The distributed log (`benchmark_query_log_dist`) records the executor `node` per
query, so `count(distinct node)` tells you immediately whether you actually got
multi-node — no waiting for `query.history` ingestion.

## 8. Serverless gotchas we hit (so you don't)

- **`databricks-sql-connector` needs `pyarrow>=14`.** On serverless the base
  pyarrow can be older than the connector's Arrow result path expects, giving
  `concat_tables() got an unexpected keyword argument 'promote_options'`. The
  notebooks `%pip install databricks-sql-connector "pyarrow>=14.0.0"`.
- **Comments do NOT bust the result cache.** Databricks normalizes SQL comments
  out of the result-cache key, so a per-query comment nonce still hits cache. The
  reliable cache control is the session setting `SET use_cached_result = false`
  (compute mode) — which works because the connector holds a persistent session
  (the stateless Statement Execution API cannot do this).
- **Use the connector, not the SDK Statement Execution API, for high QPS.** The
  SDK's shared HTTP connection pool caps real concurrency (~10), so it tops out
  around ~10–15 QPS regardless of threads. The connector opens one persistent
  connection per thread — no shared-pool cap — and reaches hundreds of QPS.
- **Explicit DataFrame schemas when writing results.** Python `int` infers as
  Spark `long`; appending to an `INT` Delta column then fails to merge. The
  results tables use explicit schemas.
- **There is a ~1s per-query floor in compute mode.** Measured on a Small
  serverless warehouse, even a point lookup runs ~1s end-to-end: ~270ms
  compilation (fixed planning overhead, *not* removed by parameterized queries),
  ~300ms execution, ~400ms fetch/queue. So **cache-off ("compute") throughput is
  capped by `max_clusters × 10 ÷ 1s`** — ~300 QPS at 30 clusters. To push
  compute-mode QPS up, **size up** (Medium/Large cut compile+exec) and/or raise
  max_clusters; to hit high QPS cheaply, use **serving mode** (the result cache
  sidesteps the per-query compute floor).
- **`system.query.history` ingests with a lag** (we saw ~8 min). The dashboard and
  the proof `COUNT(*)` are near-real-time-ish, not instant — wait a few minutes
  after a run for the authoritative numbers.
- **The run-tag comment survives even for parameterized (`:name`) queries**, so the
  proof filter works regardless of literal-vs-bound parameters.
- **One generator node caps ~130 QPS** (client-bound); reach higher QPS by running
  N generator instances that share a `run_tag` (system.query.history aggregates
  them). This is the horizontal client scale-out the benchmark is built around.

## 9. Cost levers

- Smaller size, fewer clusters, and serving-mode cache hits all cut cost.
- The warehouse `auto_stop_mins` guardrail stops idle clusters.
- Materialized views / pre-aggregates for the heaviest templates would cut cost
  further, but that is "changing the workload"; this benchmark keeps queries live.
