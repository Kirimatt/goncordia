# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [v0.5.0] — 2026-05-07

### Added
- Benchmarks for PostgreSQL, MongoDB, and Redis drivers via testcontainers (`bench/bench_containers_test.go`)
- GitHub Actions CI workflow (`go vet`, `gofmt` check, full test suite with `-race`)
- CI, Go Reference, and version badges in README

### Changed
- Updated benchmark results table in README with all 7 backends
- README code examples cleaned up: removed `{...}` placeholder syntax, fixed `intPtr` helper, unified type names

### Fixed
- Upgraded CI actions to Node.js 24 (`actions/checkout@v5`, `actions/setup-go@v6`)

---

## [v0.4.0] — 2026-05-07

### Added
- **Periodic / cron jobs**: `CronScheduler[TTx]` enqueues jobs on a configurable schedule
  - `core.Every(d time.Duration)` — fires on first tick, then every `d`
  - `core.ScheduleFunc` — plain function adapter for custom schedules
  - `CronConfig.TickInterval` and `CronConfig.Clock` for test control
- `core/schedule.go`: `Schedule` interface

---

## [v0.3.0] — 2026-05-07

### Added
- Benchmarks package (`bench/`) covering memory and SQLite drivers:
  - `BenchmarkEnqueue`, `BenchmarkEnqueueBatch100`, `BenchmarkFetchAndComplete` — raw driver path
  - `BenchmarkEndToEnd` — full WorkerPool round-trip

---

## [v0.2.0] — 2026-05-07

### Added
- **OpenTelemetry observability** (`otel/` package):
  - Span `goncordia.process` with attributes `kind`, `queue`, `id`, `attempt`
  - Histogram `goncordia.job.duration` (seconds) labelled by kind, queue, status
  - Counter `goncordia.job.count` labelled by kind, queue, status
  - `WithTracerProvider` / `WithMeterProvider` options
- `JobMiddleware` — composable middleware chain in `WorkerConfig.Middleware`
- Panic recovery inside the innermost handler: panics are converted to errors and recorded on the span; the worker pool always stays alive

---

## [v0.1.0] — 2026-05-07

### Added
- Core engine: job state machine (`available → running → completed / retryable / discarded / cancelled / scheduled`), retry policies, priority queues, unique jobs, scheduled jobs
- `Client[TTx]`: `Enqueue`, `EnqueueTx`, `EnqueueMany`, `EnqueueManyTx`, `Cancel`
- `WorkerPool[TTx]`: `Start`, `Stop`, graceful shutdown, configurable concurrency and poll interval
- `Driver[TTx]` interface parameterized by the native transaction type of each backend
- **PostgreSQL** driver (`driver/pgxv5`): pgx v5, LISTEN/NOTIFY, SKIP LOCKED, advisory locks, migrations
- **PostgreSQL / MySQL / SQLite** driver (`driver/stdlib`): `database/sql`, dialect-aware SQL
- **gorm** adapter (`driver/gorm`): thin wrapper over stdlib
- **bun** adapter (`driver/bun`): thin wrapper over stdlib
- **MongoDB** driver (`driver/mongodb`): replica set required, multi-document transactions via `mongo.SessionContext`, Change Streams notifications, migrations
- **Redis** driver (`driver/redis`): at-least-once semantics, Lua-atomic fetch via sorted sets, Pub/Sub notifications; `EnqueueTx` explicitly rejected
- **In-memory** driver (`driver/memory`): no persistence, deterministic, for tests; `AllJobs()` for state inspection
- `RetryPolicy` interface with `ExponentialRetry`, `FixedRetry`, `NoRetry` implementations
- `Clock` interface + `MockClock` for deterministic time control in tests
- MIT License

[v0.5.0]: https://github.com/kirimatt/goncordia/releases/tag/v0.5.0
[v0.4.0]: https://github.com/kirimatt/goncordia/releases/tag/v0.4.0
[v0.3.0]: https://github.com/kirimatt/goncordia/releases/tag/v0.3.0
[v0.2.0]: https://github.com/kirimatt/goncordia/releases/tag/v0.2.0
[v0.1.0]: https://github.com/kirimatt/goncordia/releases/tag/v0.1.0
