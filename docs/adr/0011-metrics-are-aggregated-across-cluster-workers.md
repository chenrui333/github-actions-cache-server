# Metrics are aggregated across cluster workers by the primary

Supersedes ADR-0007.

ADR-0007 kept `/metrics` per-process because `prom-client`'s `AggregatorRegistry.clusterMetrics()` must run in the cluster primary, and the primary looked out of reach. It is not: Nitro's `node-cluster` primary loads the app bundle and runs its plugins before it forks workers. That is the seam `plugins/setup.ts` already used for the startup banner and signal forwarding. Now that ADR-0010 makes several workers per container a supported way to use more than one core, per-worker scrapes that jump between workers are no longer an edge case.

The primary runs an `AggregatorRegistry`. Each worker registers its own registry for collection. The worker that receives a `/metrics` request asks the primary over cluster IPC. The primary collects every worker's metrics, merges them, and returns the text to that worker, which serves it. Without a cluster (`node-server`, tests), and in a cluster of one worker (the image's default `NITRO_CLUSTER_WORKERS=1`), `/metrics` serves the local registry as before. A failed or timed-out aggregation (prom-client allows each worker 5 s) is a failed scrape, never a partial one.

Merging uses each metric's aggregator. Counters, histograms, and per-worker gauges such as in-flight Merges or socket counts are summed. `s3_http_max_sockets` is summed too, because each worker has its own socket pools. Gauges computed from the shared database (`cache_storage_bytes`, `cache_leases`) use `max`, so N workers do not report the same rows N times, and so does `cache_merge_oldest_age_seconds`. Default process metrics keep prom-client's own aggregators. `process_start_time_seconds` is omitted from the merged output.

Two consequences remain. A worker that restarts takes its counters with it, so the sums drop, which Prometheus' `rate()` treats as a counter reset. Aggregated output has no per-worker breakdown either: to tell one saturated worker from an evenly loaded pool, read `nodejs_eventloop_lag_max_seconds` (aggregated with `max`) rather than the average lag.
