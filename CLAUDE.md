# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Venice is LinkedIn's derived data storage platform for planet-scale workloads. It provides high throughput async ingestion from Hadoop/Samza, low latency online reads, active-active replication with CRDT conflict resolution, and multi-cluster/multi-tenant support. Primary use case: stateful backend for Feature Stores (ML inference).

## Build Commands

```bash
./gradlew clean assemble              # Build project (also sets up git hooks)
./gradlew check --continue            # Run all checks (spotbugs + tests)
./gradlew test                        # Run unit tests
./gradlew :clients:venice-client:test # Run tests for a specific module
./gradlew test --tests "com.linkedin.venice.SomeTest"  # Run single test class
./gradlew test --tests "*.SomeTest.testMethod"         # Run single test method
./gradlew flakyTest                   # Run tests marked as flaky
./gradlew alpiniUnitTest              # Run Alpini router framework tests
./gradlew spotbugs                    # Run static analysis
./gradlew spotlessApply               # Auto-format code
./gradlew idea                        # Set up IntelliJ project
```

Test configuration: Tests use TestNG with automatic retry (up to 4 retries). Max parallel forks = 4, heap = 1g-4g.

## Module Structure

**Clients** (`clients/`):
- `da-vinci-client` - Stateful client with eager local caching
- `venice-thin-client` - Minimal remote read client (via router)
- `venice-client` - Unified client library (includes thin, fast, da-vinci)
- `venice-producer` - Real-time write capabilities
- `venice-push-job` - Batch data push from Hadoop
- `venice-admin-tool` - CLI for operators
- `venice-samza` - Samza system producer

**Services** (`services/`):
- `venice-controller` - Control plane (store creation, schema evolution, cluster management)
- `venice-router` - Stateless routing tier for thin-client queries
- `venice-server` - Stateful data hosting tier (RocksDB storage)
- `venice-standalone` - All-in-one deployment

**Internal** (`internal/`):
- `venice-common` - Core utilities and APIs
- `venice-client-common` - Minimal client APIs
- `venice-test-common` - Shared test utilities
- `alpini/` - Netty-based router framework (forked from LinkedIn Espresso)

**Integrations** (`integrations/`): Beam, DuckDB, Pulsar, Samza connectors

## Architecture Concepts

**Write Path**:
- Version Topic (VT): Kafka topic holding all data for a store version
- Real-time Topic (RT): Kafka topic for nearline/online producer data
- Store Ingestion Task (SIT): Per-store-version handler on servers
- Hybrid stores: Mix batch (VT) and real-time (RT) data

**Read Path**:
- Single/batch get and read compute (dot product, cosine similarity, Hadamard product)
- Client modes: Thin Client (2 hops) → Fast Client (1 hop) → Da Vinci (0 hops, local cache)

**Cluster Management**: ZooKeeper for metadata, Helix for partition management

## Code Style

Code is auto-formatted via Spotless (Eclipse Java Formatter) on git commit. Run `./gradlew assemble` to set up hooks.

Key conventions:
- **Compatibility**: Old clients must work with new servers and vice versa. Use `VeniceEnumValue`/`EnumUtils` for cross-process enums
- **No wildcard imports**: Use specific imports
- **No Optional**: Use null or sentinel values instead
- **No Stream API in hot paths**: Use traditional for loops for performance
- **No double-brace initialization**: Avoid `new HashMap<>() {{ put(...); }}`
- **Logging**: Use log4j2 with interpolation, not string concatenation. No hot-path logging above DEBUG
- **Data structures**: Use arrays over maps when possible; use fastutil for primitive collections

## PR Requirements

Title format: `[component1]...[componentN] Concise message`

Valid tags: `[da-vinci]`/`[dvc]`, `[server]`, `[controller]`, `[router]`, `[samza]`, `[vpj]`, `[fast-client]`/`[fc]`, `[thin-client]`/`[tc]`, `[changelog]`/`[cc]`, `[pulsar-sink]`, `[producer]`, `[admin-tool]`, `[test]`, `[build]`, `[doc]`, `[script]`, `[compat]`, `[protocol]`

Example: `[server][da-vinci] Use dedicated thread to persist data to storage engine`

## Key Dependencies

- Avro 1.10.2 (with LinkedIn avroutil1)
- Kafka 2.4.1
- RocksDB 9.11.2
- gRPC 1.59.1
- Protobuf 3.24.0
- Netty 4.1.74
- ZooKeeper 3.6.3
- TestNG 6.14.3

## Metrics Architecture

Venice uses dual metrics: **Tehuti** (legacy Kafka-style) and **OpenTelemetry** (modern dimensional). Full documentation: `docs/metrics-categorization.md`

### Metric Entity Files (OTel)
- `RouterMetricEntity` - Router request/retry metrics (`services/venice-router/.../stats/`)
- `ServerMetricEntity` - Server ingestion metrics (`clients/da-vinci-client/.../stats/`)
- `ControllerMetricEntity` - Control plane metrics (`services/venice-controller/.../stats/`)
- `ClientMetricEntity` / `BasicClientMetricEntity` - Thin client metrics (`clients/venice-thin-client/.../stats/`)
- `FastClientMetricEntity` / `ClusterMetricEntity` - Fast client metrics (`clients/venice-client/.../fastclient/stats/`)
- `RoutingMetricEntity` - Helix group routing (`internal/venice-client-common/.../stats/routing/`)
- `RetryManagerMetricEntity` - Retry rate limiting (`internal/venice-common/.../stats/`)
- `BasicConsumerMetricEntity` - Changelog consumer (`clients/da-vinci-client/.../consumer/stats/`)

### Key Stats Classes (Tehuti)
- `HostLevelIngestionStats` - Ingestion throughput, leader/follower consumption, DCR metrics
- `ServerHttpRequestStats` - Server read latency, storage engine query, read compute
- `BlobTransferStats` - Blob transfer throughput

### Metrics by Category
- **Read Path**: `call_count`, `call_time`, `storage_engine_query_latency`, `read_compute_*`
- **Write/Ingestion**: `bytes_consumed`, `records_consumed`, `leader_*`, `follower_*`, `storage_engine_put_latency`
- **Hybrid/RT**: `{region}_rt_bytes_consumed`, `storage_quota_used`, `ingestion.replication.heartbeat.delay`
- **DCR (Active-Active)**: `update_ignored_dcr`, `tombstone_creation_dcr`, `timestamp_regression_dcr_error`
- **Retry**: `retry_count`, `allowed_retry_count`, `retry.rate_limit.*`

### Standard Dimensions
`venice.store.name`, `venice.cluster.name`, `venice.request.method`, `http.response.status_code`, `venice.response.status_code_category`, `venice.region.name`, `venice.version.role`
