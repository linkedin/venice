# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Venice is a derived data storage platform designed for planet-scale workloads. It provides high throughput asynchronous ingestion from batch and streaming sources, low latency online reads via remote queries or in-process caching, and active-active replication between regions with CRDT-based conflict resolution.

## Build Commands

### Primary Build Commands
```bash
# Build project classes and artifacts
./gradlew assemble

# Clean and build everything
./gradlew clean assemble

# Run all checks (spotbugs, code coverage, tests)
./gradlew check --continue

# Run only spotbugs static analysis
./gradlew spotbugs

# Run code coverage verification
./gradlew jacocoTestCoverageVerification
```

### Module-Specific Commands
```bash
# Build a specific module
./gradlew :clients:da-vinci-client:assemble

# Run spotbugs for a specific module
./gradlew :clients:da-vinci-client:spotbugsMain

# Run tests for a specific module
./gradlew :clients:da-vinci-client:test
```

## Testing

### Running Tests
```bash
# Run all tests
./gradlew check --continue

# Run a specific test
./gradlew :sub-module:testType --tests "fully.qualified.name.of.the.test"

# Run a specific integration test
./gradlew :internal:venice-test-common:integrationTest --tests "fully.qualified.name.of.the.test"
```

### Test Organization
- **Unit tests**: Located in `src/test/java` of each module
- **Integration tests**: Located in `src/integrationTest/java` (primarily in `venice-test-common`)
- **Test infrastructure**: `internal/venice-test-common` provides shared test utilities, mock pub/sub implementations, and TestNG-based test frameworks

### Test Configuration
- Tests use TestNG framework
- Integration tests may require setting `pubSubBrokerFactory` system property
- Separate test tasks exist for Alpini tests vs Venice tests
- Tests run with specific JVM args for Java 17 compatibility (memory access flags)

## Architecture Overview

### Module Structure

**Clients** (`/clients`) - User-facing libraries:
- `da-vinci-client`: Stateful client with eager caching (local RAM+SSD storage) and streaming updates. Provides <1ms latency reads
- `venice-thin-client`: Minimal stateless client for remote queries via router (2 network hops, ~10ms p99)
- `venice-fast-client`: Partitioning-aware client that bypasses router (1 network hop, ~2ms p99)
- `venice-client`: Convenience wrapper aggregating thin-client, fast-client, and da-vinci-client
- `venice-producer`: Real-time write capability for per-service nearline producers
- `venice-push-job`: Batch data ingestion via Hadoop/Spark MapReduce
- `venice-admin-tool`: CLI tool for administrative operations

**Services** (`/services`) - Deployable backend components:
- `venice-controller`: Control plane managing dataset lifecycle, schema evolution, cluster coordination via Helix/ZooKeeper, push monitoring, and authorization
- `venice-server`: Stateful data tier hosting partitions, serving read requests, ingesting from Kafka, using RocksDB for storage
- `venice-router`: Stateless query router handling thin-client requests, metadata queries, ACL enforcement, and throttling. Built on Alpini framework
- `venice-standalone`: All-in-one deployment combining controller, router, and server

**Internal** (`/internal`) - Non-public shared libraries:
- `venice-common`: Shared utilities, configuration, Helix integration, controller API definitions, exception hierarchy
- `venice-client-common`: Minimal shared APIs for clients (schema utilities, minimal dependencies)
- `alpini`: Netty-based async routing framework (forked from LinkedIn's Espresso) powering the router tier
- `venice-test-common`: Shared test infrastructure and utilities

**Integrations** (`/integrations`) - Third-party system bridges:
- `venice-samza`: Samza SystemProducer for stream processing jobs
- `venice-pulsar`: Pulsar Sink implementation
- `venice-beam`: Apache Beam Read API
- `venice-duckdb`: DuckDB integration for SQL queries

### Key Architectural Patterns

**Write Path**:
- Three granularities: full dataset swap, row insertion, column updates
- VeniceWriter handles chunking for large values (>1MB Kafka limit)
- StoreIngestionTask orchestrates server-side ingestion (one per store version)
- Hybrid stores combine batch pushes with real-time writes using rewind time
- Write Compute supports partial updates and collection merging (currently active-passive only)

**Read Path**:
- Thin client: 2 hops (client→router→server)
- Fast client: 1 hop (client→server) with minimal metadata
- Da Vinci: 0 hops (local cache) with streaming updates and record transformers

**Storage & Replication**:
- RocksDB with ZSTD compression for key-value storage
- Partition-per-server model via Helix coordination
- Active-active replication with CRDT-based conflict resolution
- Cross-region coordination via controller

**Data Flow**:
- Kafka topics: Version Topic (VT) for full store data, Real-time Topic (RT) for nearline updates
- KafkaStoreIngestionService consumes from topics → StoreIngestionTask handles partition ingestion → RocksDB writes

## Code Style and Conventions

### Compatibility First
All cross-process or cross-lifetime data must use versioned enums and protocols. Use `VeniceEnumValue` and `EnumUtils` for enums communicated across processes or lifetimes to ensure numeric ID stability.

### Code Formatting
- **Spotless**: Eclipse Java Formatter automatically reformats code on commit. Run `./gradlew assemble` to set up git hooks
- Wildcard imports should be disabled (see workspace_setup.md)

### Static Analysis
- **SpotBugs**: Enforced rules defined in `gradle/spotbugs/include.xml`
- Build fails if violations detected
- Run with `./gradlew spotbugs`

### Logging
- Use log4j2 with interpolation syntax, never manual string concatenation: `logger.info("Value: {}", value)` not `logger.info("Value: " + value)`
- Hot path logging should be debug level or converted to metrics
- Use `RedundantExceptionFilter` for high-frequency exception logging

### JavaDoc
- Required for all public client APIs
- Class-level JavaDoc should describe responsibilities
- Use single-line format for short descriptions: `/** Short description */`
- Use `{@link ClassName}` syntax for navigability (even outside standard JavaDoc locations)

### Performance Considerations
- Minimize boxing in hot paths
- Memory-conscious data structure design
- Lazy deserialization in Da Vinci (keys/values deserialized only on access)

## Development Workflow

### Creating Pull Requests
When creating a pull request, you MUST follow the template in `.github/pull_request_template.md`. This is a strict requirement - PRs not following the format will not be merged.

### Pull Request Process
1. Create GitHub issue first (unless trivial bug fix or doc change)
2. For major changes, create a design document following the Design Document Guide
3. Fork the repo and create a new branch
4. Run full test suite: `./gradlew check --continue`
5. **PR Title Format**: `[component1]...[componentN] Concise message` (NO colon after tags)
   - Valid tags: `[da-vinci]` (or `[dvc]`), `[server]`, `[controller]`, `[router]`, `[samza]`, `[vpj]`, `[fast-client]` (or `[fc]`), `[thin-client]` (or `[tc]`), `[changelog]` (or `[cc]`), `[pulsar-sink]`, `[producer]`, `[admin-tool]`, `[test]`, `[build]`, `[doc]`, `[script]`, `[compat]`, `[protocol]`
   - Example: `[server][da-vinci] Use dedicated thread to persist data to storage engine`
   - Use `[compat]` tag for compatibility-related changes that require specific deployment order
6. **PR Description Template** - Follow the structure in `.github/pull_request_template.md`:

   **Problem Statement**: Describe what problem you're solving, what issues exist in current code, and why the change is necessary

   **Solution**: Explain what changes you're making and why, how they solve the problem, performance considerations, and testing performed

   **Code Changes Checklist**:
   - Check if new code is behind a config (list config names and defaults)
   - Check if new log lines are introduced (confirm if rate limiting is needed)

   **Concurrency-Specific Checks** (both reviewer and author verify):
   - No race conditions or thread safety issues
   - Proper synchronization mechanisms (synchronized, RWLock) used where needed
   - No blocking calls inside critical sections
   - Thread-safe collections verified (ConcurrentHashMap, CopyOnWriteArrayList)
   - Proper exception handling in multi-threaded code

   **Testing**:
   - List new unit tests added
   - List new integration tests added
   - Note any modified/extended existing tests
   - Verify backward compatibility

   **Breaking Changes**:
   - Indicate if there are user-facing or breaking changes
   - If yes, explain previous behavior vs new behavior

7. Include `Resolves #XXX` in description to link issues
8. Address review feedback via additional commits (not amends) for easier review tracking

### Requirements
- Java 17 for development (compile targets Java 8 for compatibility)
- Set `JAVA_HOME` environment variable
- File descriptor soft limit: ≥64000, hard limit: ≥524288
- Run `./gradlew idea` to set up IntelliJ IDEA

### IDE Setup
- IntelliJ IDEA recommended (Community Edition)
- Run `./gradlew cleanIdea idea` to configure IDE settings
- Disable dangling JavaDoc warnings (automatically configured)

## Key Dependencies

- **Build**: Gradle 7.x
- **Kafka**: v2.4.1 (Apache or LinkedIn fork)
- **Avro**: v1.10.2 with avroutil v0.4.30
- **gRPC**: v1.59.1 with protobuf v3.24.0
- **Hadoop**: v2.10.2
- **Netty**: v4.1.74 for async I/O
- **RocksDB**: v9.11.2 for storage
- **ZooKeeper**: v3.6.3 via Helix
- **Jackson**: v2.15.0 for JSON
- **TestNG**: v6.14.3 for testing

## Common Development Patterns

### Running Single Tests
```bash
# Unit test example
./gradlew :clients:da-vinci-client:test --tests "com.linkedin.davinci.client.DaVinciClientTest"

# Integration test example
./gradlew :internal:venice-test-common:integrationTest --tests "com.linkedin.venice.integration.VeniceClusterTest"
```

### Protocol Changes
When introducing new protocol versions:
1. Pin current version in `compileAvro` task via `versionOverrides`
2. Deploy and release with pinned version
3. In follow-up PR, remove override to use new protocol

### Avro Schema Evolution
- Schemas located in `src/main/resources/avro/` with versioned subdirectories (v1, v2, etc.)
- SchemaBuilder compiles latest version of each schema type
- Use `compileAvro` task for schema code generation

### Adding New Modules
Modules defined in `settings.gradle` and follow Gradle multi-project structure. New modules should:
- Include appropriate `build.gradle` with dependencies
- Follow naming convention: clients/services/internal/integrations prefix
- Include unit tests in `src/test/java`
