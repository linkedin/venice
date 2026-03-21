# Venice Feature Matrix Integration Test - Design Document

## Overview

This module implements a 3-way combinatorial integration test covering 42 feature dimensions across Venice's write path,
read path, server, controller, and router components.

## Problem Statement

Venice has 600+ configuration keys across routers, servers, controllers, push jobs, and clients. These are tested in
isolation across ~130 E2E tests. There is no systematic way to test cross-layer interactions between feature flags,
leading to coverage gaps for multi-flag interaction bugs.

## Solution

Use PICT (Pairwise Independent Combinatorial Testing) to generate a covering array of test cases that exercises all
3-way interactions across 42 feature dimensions, producing approximately 300-500 test cases.

## Feature Maturity

Each dimension is classified by maturity level:

| Level            | Meaning                                                      | PICT Treatment            |
| ---------------- | ------------------------------------------------------------ | ------------------------- |
| **MATURE**       | Always-on in production; default=true or universally enabled | Fixed to production value |
| **MATURING**     | Active development; important to test both on/off states     | Varied by PICT            |
| **EXPERIMENTAL** | New features; important to test both on/off states           | Varied by PICT            |

Fixing 9 MATURE dimensions to their production value reduces PICT's combinatorial space and the number of unique cluster
configurations needed.

## 42-Dimension Feature Matrix

### Write Path (W1-W13)

| ID  | Dimension             | Values                              | Maturity | Constraint                         |
| --- | --------------------- | ----------------------------------- | -------- | ---------------------------------- |
| W1  | Data Flow Topology    | Batch-only / Hybrid / Nearline-only | MATURING | --                                 |
| W2  | Native Replication    | **on** (fixed)                      | MATURE   | Implied by W3=on                   |
| W3  | Active-Active         | **on** (fixed)                      | MATURE   | Always-on in production            |
| W4  | Write Compute         | on / off                            | MATURING | W3=on                              |
| W5  | Chunking              | on / off                            | MATURING | Auto when W4=on                    |
| W6  | RMD Chunking          | on / off                            | MATURING | W5=on AND W3=on                    |
| W7  | Incremental Push      | on / off                            | MATURING | W3=on                              |
| W8  | Compression           | NO_OP / GZIP / ZSTD_WITH_DICT       | MATURING | ZSTD invalid when W1=Nearline-only |
| W9  | Deferred Version Swap | on / off                            | MATURING | C5=on                              |
| W10 | Target Region Push    | on / off                            | MATURING | Multi-region only                  |
| W11 | Push Engine           | MapReduce / Spark native            | MATURING | --                                 |
| W12 | TTL Repush            | on / off                            | MATURING | Kafka source repush                |
| W13 | Separate RT Topic     | on / off                            | MATURING | W7=on                              |

### Read Path (R1-R6)

| ID  | Dimension                  | Values                               | Maturity | Constraint               |
| --- | -------------------------- | ------------------------------------ | -------- | ------------------------ |
| R1  | Client Type                | Thin / Fast / DaVinci                | MATURING | --                       |
| R2  | Read Compute               | on / off                             | MATURING | Store readComputeEnabled |
| R3  | DaVinci Storage Class      | MEMORY_BACKED_BY_DISK / MEMORY / N/A | MATURING | R1=DaVinci               |
| R4  | Fast Client Routing        | LEAST_LOADED / HELIX_ASSISTED / N/A  | MATURING | R1=Fast                  |
| R5  | Long-tail Retry (client)   | on / off                             | MATURING | R1=Thin or Fast          |
| R6  | DaVinci Record Transformer | on / off / N/A                       | MATURING | R1=DaVinci               |

### Server (S1-S6)

| ID  | Dimension                 | Values         | Maturity | Constraint      |
| --- | ------------------------- | -------------- | -------- | --------------- |
| S1  | Parallel Batch Get        | on / off       | MATURING | --              |
| S2  | Fast Avro                 | **on** (fixed) | MATURE   | default=true    |
| S3  | AA/WC Parallel Processing | on / off       | MATURING | W3=on AND W4=on |
| S4  | Blob Transfer             | on / off       | MATURING | --              |
| S5  | Quota Enforcement         | on / off       | MATURING | --              |
| S6  | Adaptive Throttler        | on / off       | MATURING | --              |

### Controller (C1-C9)

| ID  | Dimension                         | Values         | Maturity | Constraint   |
| --- | --------------------------------- | -------------- | -------- | ------------ |
| C1  | AA Default for Hybrid             | **on** (fixed) | MATURE   | Always-on    |
| C2  | WC Auto for Hybrid+AA             | on / off       | MATURING | --           |
| C3  | Inc Push Auto for Hybrid+AA       | on / off       | MATURING | --           |
| C4  | Separate RT Auto for Inc Push     | on / off       | MATURING | --           |
| C5  | Deferred Version Swap Service     | on / off       | MATURING | --           |
| C6  | Schema Validation                 | **on** (fixed) | MATURE   | default=true |
| C7  | Backup Version Cleanup            | **on** (fixed) | MATURE   | Always-on    |
| C8  | System Store Auto-Materialization | on / off       | MATURING | --           |
| C9  | Superset Schema Generation        | on / off       | MATURING | --           |

### Router (RT1-RT8) — EXCLUDED from PICT

Router properties are not yet supported in `VeniceMultiRegionClusterCreateOptions`. All RT dimensions use their defaults
until `routerProperties` support is added (see `memory/router-properties-plan.md`).

| ID  | Dimension             | Values                        | Maturity | Constraint | Status   |
| --- | --------------------- | ----------------------------- | -------- | ---------- | -------- |
| RT1 | Read Throttling       | on / off                      | MATURE   | --         | Excluded |
| RT2 | Early Throttle        | on / off                      | MATURING | RT1=on     | Excluded |
| RT3 | Smart Long-tail Retry | on / off                      | MATURE   | --         | Excluded |
| RT4 | Routing Strategy      | LEAST_LOADED / HELIX_ASSISTED | MATURING | --         | Excluded |
| RT5 | Latency-based Routing | on / off                      | MATURING | --         | Excluded |
| RT6 | Client Decompression  | on / off                      | MATURE   | --         | Excluded |
| RT7 | HTTP/2 Inbound        | on / off                      | MATURING | --         | Excluded |
| RT8 | Connection Warming    | on / off                      | MATURING | --         | Excluded |

## Architecture

### Cluster Grouping Strategy

Server, router, and controller configs are set at startup and cannot change per-store. Test cases are grouped by unique
(S1-S6, RT1-RT8, C1-C9) tuples. One cluster instance is created per unique tuple.

With 9 MATURE dimensions fixed (S2, C1, C6, C7, RT1, RT3, RT6 plus W2, W3), only 16 S/C/RT dimensions vary. At 2-way
coverage this produces ~22 test cases with ~21 unique cluster configs. At 3-way coverage with more test cases, cluster
sharing improves significantly.

### Test Flow

```
FeatureMatrixIntegrationTest
|-- @Factory(dataProvider = "clusterConfigs")
|   Creates one test instance per unique (S,RT,C) cluster config
|
|-- @BeforeClass
|   FeatureMatrixClusterSetup.create()
|   Creates multi-region cluster via ServiceFactory
|
|-- @DataProvider("storeAndClientMatrix")
|   Returns all (W,R) test cases for this cluster's (S,RT,C) config
|
|-- @Test(dataProvider = "storeAndClientMatrix")
|   For each (W,R) combination:
|   1. StoreConfigurator.create() - store with W flags
|   2. Write data - BatchPush/Streaming/IncrementalPush
|   3. Validate reads - DataIntegrity/ReadCompute/WriteCompute
|   4. Cleanup store
|
|-- @AfterClass
    Tear down cluster
```

### Dependency Chains

**Chain 1: Replication Hierarchy**

```
Native Replication (NR)
  -- Active-Active (AA) [requires NR]
       |-- Incremental Push [requires AA]
       |-- Write Compute [auto for hybrid+AA]
       |    |-- Chunking [auto when WC]
       |    -- RMD Chunking [auto when WC+AA]
       -- Separate RT Topic [optional, for inc push]
```

**Chain 2: Hybrid Store** - HybridStoreConfig -> RT Topic, Separate RT, Disk Quota

**Chain 3: Compression** - ZSTD_WITH_DICT -> Dict training -> Router dict retrieval -> Client decompression

**Chain 4: Blob Transfer** - Store flag -> Version flag -> Server flag (cross-layer)

**Chain 5: Chunking** - chunkingEnabled -> rmdChunkingEnabled (requires chunking + AA)

### Failure Classification

| Validation Step Fails At   | Likely Component         |
| -------------------------- | ------------------------ |
| Store creation             | Controller               |
| Batch push                 | PushJob / Server         |
| RT write / ingestion       | Server (ingestion)       |
| Incremental push           | Server / Controller      |
| Single get returns null    | Server / Router          |
| Single get wrong value     | Server (data corruption) |
| Batch get missing keys     | Router (scatter-gather)  |
| Read compute wrong result  | Server (compute engine)  |
| Write compute wrong result | Server (AA/WC merge)     |
| Decompression failure      | Router / Client          |
| Chunk reassembly failure   | Client / Server          |

## Reporting

- **HTML Report**: `build/reports/feature-matrix-report.html`

  - Failures by component
  - Failures by dimension value (top 20)
  - Failures by dimension pair (top 20)
  - Individual failure details

- **JSON Report**: `build/reports/feature-matrix-results.json`
  - Structured data for future regression tooling
  - Deterministic test case IDs for run-to-run comparison
