# Venice Metrics Categorization

## Overview

Venice uses a dual-metric system:
- **Tehuti**: Legacy metrics framework (Kafka-style sensor/stat)
- **OpenTelemetry (OTel)**: Modern metrics with dimensional labels

---

## 1. Categorization by Component

### **Router Metrics** (`RouterMetricEntity`)
| Metric | Type | Description |
|--------|------|-------------|
| `call_count` | Counter | Request count with response codes |
| `call_time` | Histogram | Request latency |
| `call_size` | Histogram | Request/response size in bytes |
| `key_size` | Histogram | Key size in bytes |
| `key_count` | Histogram | Keys per request |
| `retry_count` | Counter | Retry requests triggered |
| `allowed_retry_count` | Counter | Allowed retries |
| `disallowed_retry_count` | Counter | Disallowed retries |
| `aborted_retry_count` | Counter | Aborted retries |
| `retry_delay` | Aggregation | Time between original and retry |

### **Server/Storage Metrics** (`ServerMetricEntity`, `ServerHttpRequestStats`)
| Metric | Type | Description |
|--------|------|-------------|
| `ingestion.replication.heartbeat.delay` | Histogram | Nearline replication lag |
| `success_request` | Rate | Successful request rate |
| `error_request` | Rate | Error request rate |
| `success_request_latency` | Percentile | Success latency |
| `error_request_latency` | Percentile | Error latency |
| `storage_engine_query_latency` | Percentile | DB lookup latency |
| `storage_engine_query_latency_for_small_value` | Percentile | Small value lookup |
| `storage_engine_query_latency_for_large_value` | Percentile | Large value lookup |
| `request_key_count` | Histogram | Keys per request |
| `key_not_found` | Rate | Key not found rate |
| `multi_chunk_large_value_count` | Gauge | Large values assembled |
| `read_compute_latency` | Percentile | Compute operation latency |
| `dot_product_count` | Counter | Dot product operations |
| `cosine_similarity` | Counter | Cosine similarity ops |
| `hadamard_product` | Counter | Hadamard product ops |

### **Controller Metrics** (`ControllerMetricEntity`)
| Metric | Type | Description |
|--------|------|-------------|
| `inflight_call_count` | UpDownCounter | Current in-flight calls |
| `call_count` | Counter | Total calls to Spark server |
| `call_time` | Histogram | Call latency |
| `store.repush.call_count` | Counter | Store repush requests |
| `store.compaction.nominated_count` | Counter | Stores nominated for compaction |
| `store.compaction.eligible_state` | Gauge | Compaction eligibility state |
| `store.compaction.triggered_count` | Counter | Compaction repush triggered |

### **Client Metrics** (`ClientMetricEntity`, `BasicClientMetricEntity`, `FastClientMetricEntity`)

**Thin/Fast Client:**
| Metric | Type | Description |
|--------|------|-------------|
| `call_count` | Counter | Request count with HTTP status |
| `call_time` | Histogram | Request latency |
| `request.key_count` | Histogram | Keys in request |
| `response.key_count` | Histogram | Keys in response |
| `retry.call_count` | Counter | Retry request count |
| `retry.request.key_count` | Aggregation | Keys in retry requests |
| `retry.response.key_count` | Aggregation | Keys in retry responses |
| `request.serialization_time` | Histogram | Serialization time |
| `response.decompression_time` | Histogram | Decompression time |
| `response.deserialization_time` | Histogram | Deserialization time |
| `route.call_count` | Counter | Requests per route/instance |
| `route.call_time` | Histogram | Route latency |
| `request.timeout.count` | Counter | Client-side timeouts |

**Fast Client Specific:**
| Metric | Type | Description |
|--------|------|-------------|
| `retry.request.win_count` | Counter | Retries that outperformed original |
| `metadata.staleness_duration` | AsyncGauge | Metadata freshness |
| `request.fanout_count` | Aggregation | Fanout size distribution |
| `request.rejection_count` | Counter | Rejected requests |
| `request.rejection_ratio` | Aggregation | Rejection ratio |

**Da Vinci Client:**
| Metric | Type | Description |
|--------|------|-------------|
| `call_count` (DVC variant) | Counter | Local read count |
| `call_time` (DVC variant) | Histogram | Local read latency |

### **Cluster/Routing Metrics** (`ClusterMetricEntity`, `RoutingMetricEntity`)
| Metric | Type | Description |
|--------|------|-------------|
| `store.version.update_failure_count` | Counter | Version update failures |
| `instance.error_count` | Aggregation | Instance errors (blocked/unhealthy) |
| `store.version.current` | AsyncGauge | Current version served |
| `helix_group.count` | Aggregation | Available Helix groups |
| `helix_group.call_count` | Counter | Requests per Helix group |
| `helix_group.request.pending_requests` | Aggregation | Pending requests per group |
| `helix_group.call_time` | Aggregation | Response time per group |

### **Retry Manager Metrics** (`RetryManagerMetricEntity`)
| Metric | Type | Description |
|--------|------|-------------|
| `retry.rate_limit.target_tokens` | AsyncGauge | Rate limit tokens/sec |
| `retry.rate_limit.remaining_tokens` | AsyncGauge | Remaining retry budget |
| `retry.rate_limit.rejection_count` | Counter | Rejected retry operations |

---

## 2. Categorization by Read/Write/Control

### **Read Path Metrics**
- Router: `call_count`, `call_time`, `key_count`, `retry_*`
- Server: `success_request`, `storage_engine_query_latency`, `read_compute_*`, `dot_product_count`, `cosine_similarity`, `hadamard_product`
- Client: `call_count`, `call_time`, `request.key_count`, `response.key_count`, `route.*`
- Da Vinci: `call_count_dvc`, `call_time_dvc`

### **Write Path / Ingestion Metrics** (`HostLevelIngestionStats`)
| Metric | Category | Description |
|--------|----------|-------------|
| `bytes_consumed` | Ingestion | Total bytes consumed from Kafka |
| `records_consumed` | Ingestion | Records consumed rate |
| `leader_bytes_consumed` | Leader Ingestion | Bytes consumed by leader |
| `leader_records_consumed` | Leader Ingestion | Records consumed by leader |
| `follower_bytes_consumed` | Follower Ingestion | Bytes consumed by follower |
| `follower_records_consumed` | Follower Ingestion | Records consumed by follower |
| `leader_bytes_produced` | Leader Production | Bytes produced by leader |
| `leader_records_produced` | Leader Production | Records produced by leader |
| `storage_engine_put_latency` | Storage | Put operation latency |
| `storage_engine_delete_latency` | Storage | Delete operation latency |
| `leader_produce_latency` | Production | Leader produce latency |
| `leader_compress_latency` | Compression | Compression latency |
| `ingestion_failure` | Error | Ingestion failure count |
| `checksum_verification_failure` | Error | Checksum failures |

### **Control Plane Metrics**
- Controller: `call_count`, `call_time`, `inflight_call_count`, `store.repush.*`, `store.compaction.*`
- Version Management: `store.version.update_failure_count`, `store.version.current`
- Metadata: `metadata.staleness_duration`

---

## 3. Categorization by Store Type

### **Hybrid Store Metrics** (Real-time + Batch)
| Metric | Description |
|--------|-------------|
| `{region}_rt_bytes_consumed` | RT topic bytes consumed per region |
| `{region}_rt_records_consumed` | RT topic records consumed per region |
| `storage_quota_used` | Hybrid quota usage percentage |
| `ingestion.replication.heartbeat.delay` | Nearline replication lag |

### **Batch Store Metrics**
| Metric | Description |
|--------|-------------|
| `bytes_consumed` | Version topic bytes consumed |
| `records_consumed` | Version topic records consumed |
| `disk_usage_in_bytes` | Storage disk usage |
| `batch_processing_request*` | Batch processing metrics |

### **Nearline/Real-time Metrics**
| Metric | Description |
|--------|-------------|
| `ingestion.replication.heartbeat.delay` | Heartbeat-based lag tracking |
| `leader_ingestion_*` | Leader-specific ingestion metrics |
| `follower_*` | Follower consumption metrics |

---

## 4. Categorization by Feature

### **Active-Active Replication (DCR) Metrics**
| Metric | Description |
|--------|-------------|
| `update_ignored_dcr` | Updates ignored due to conflict resolution |
| `tombstone_creation_dcr` | Tombstones created for DCR |
| `timestamp_regression_dcr_error` | Timestamp regression errors |
| `offset_regression_dcr_error` | Offset regression errors |
| `leader_ingestion_active_active_put_latency` | A/A put latency |
| `leader_ingestion_active_active_update_latency` | A/A update latency |
| `leader_ingestion_active_active_delete_latency` | A/A delete latency |

### **Write Compute Metrics**
| Metric | Description |
|--------|-------------|
| `leader_write_compute_lookup_latency` | WC data lookup latency |
| `leader_write_compute_update_latency` | WC actual update latency |
| `write_compute_cache_hit_count` | Transient record cache hits |
| `read_compute_latency` | Read compute execution time |
| `read_compute_deserialization_latency` | Deserialization time |
| `read_compute_serialization_latency` | Serialization time |
| `read_compute_efficiency` | Compute efficiency ratio |

### **Read Compute Operations**
| Metric | Description |
|--------|-------------|
| `dot_product_count` | Dot product operations |
| `cosine_similarity` | Cosine similarity operations |
| `hadamard_product` | Hadamard product operations |
| `count_operator` | Count operations |

### **Blob Transfer Metrics**
| Metric | Description |
|--------|-------------|
| `blob_transfer_bytes_sent` | Bytes sent via blob transfer |
| `blob_transfer_bytes_received` | Bytes received via blob transfer |

### **Changelog Consumer Metrics** (`BasicConsumerMetricEntity`)
| Metric | Description |
|--------|-------------|
| `heart_beat_delay` | Max heartbeat delay across partitions |
| `current_consuming_version` | Min/max consuming version |
| `records_consumed_count` | Records consumed count |
| `poll_count` | Poll invocation count |
| `version_swap_count` | Version swap count |
| `chunked_record_count` | Chunked records consumed |

### **Large Value Handling**
| Metric | Description |
|--------|-------------|
| `multi_chunk_large_value_count` | Large values assembled |
| `storage_engine_query_latency_for_large_value` | Large value lookup latency |
| `storage_engine_query_latency_for_small_value` | Small value lookup latency |
| `assembled_record_size_in_bytes` | Assembled record size |
| `assembled_record_size_ratio` | Assembled vs original ratio |
| `assembled_rmd_size_in_bytes` | Assembled RMD size |

### **Retry/Resilience Metrics**
| Metric | Description |
|--------|-------------|
| `retry_count` | Total retries |
| `allowed_retry_count` | Allowed retries |
| `disallowed_retry_count` | Rate-limited retries |
| `aborted_retry_count` | Aborted retries |
| `retry.request.win_count` | Winning retries (fast client) |
| `retry.rate_limit.*` | Retry rate limiting |

### **Storage Engine Metrics**
| Metric | Description |
|--------|-------------|
| `disk_usage_in_bytes` | Total disk usage |
| `rmd_disk_usage_in_bytes` | RMD disk usage |
| `storage_engine_put_latency` | Put latency |
| `storage_engine_delete_latency` | Delete latency |
| `storage_engine_query_latency` | Query latency |

---

## 5. Metric Dimensions (Labels)

All OTel metrics use standardized dimensions:

| Dimension | Description |
|-----------|-------------|
| `venice.store.name` | Store name |
| `venice.cluster.name` | Cluster name |
| `venice.request.method` | Request type (single_get, multi_get, compute) |
| `http.response.status_code` | HTTP status (200, 400, etc.) |
| `http.response.status_code_category` | HTTP category (2xx, 4xx, 5xx) |
| `venice.response.status_code_category` | Venice status (SUCCESS, FAIL) |
| `venice.request.retry_type` | Retry type (long_tail, error) |
| `venice.region.name` | Datacenter/region |
| `venice.version.role` | Version role (CURRENT, BACKUP, FUTURE) |
| `venice.replica.type` | Replica type |
| `venice.replica.state` | Replica state |
| `venice.helix_group.id` | Helix group ID |

---

## 6. Summary Statistics

- **Total Metric Entity Enums**: 10 core definitions
- **Total Stats Classes**: 50+ specialized classes
- **Component Distribution**:
  - Router: ~10 metrics
  - Server/Ingestion: ~50+ metrics
  - Controller: ~7 metrics
  - Clients: ~25+ metrics
  - Common/Utility: ~10+ metrics

---

## 7. Key Source Files

### Metric Entity Definitions
- `services/venice-router/src/main/java/com/linkedin/venice/router/stats/RouterMetricEntity.java`
- `clients/da-vinci-client/src/main/java/com/linkedin/davinci/stats/ServerMetricEntity.java`
- `services/venice-controller/src/main/java/com/linkedin/venice/controller/stats/ControllerMetricEntity.java`
- `clients/venice-thin-client/src/main/java/com/linkedin/venice/client/stats/ClientMetricEntity.java`
- `clients/venice-thin-client/src/main/java/com/linkedin/venice/client/stats/BasicClientStats.java`
- `clients/venice-client/src/main/java/com/linkedin/venice/fastclient/stats/FastClientMetricEntity.java`
- `clients/venice-client/src/main/java/com/linkedin/venice/fastclient/stats/ClusterMetricEntity.java`
- `internal/venice-client-common/src/main/java/com/linkedin/venice/stats/routing/RoutingMetricEntity.java`
- `internal/venice-common/src/main/java/com/linkedin/venice/stats/RetryManagerMetricEntity.java`
- `clients/da-vinci-client/src/main/java/com/linkedin/davinci/consumer/stats/BasicConsumerStats.java`

### Stats Implementation Classes
- `clients/da-vinci-client/src/main/java/com/linkedin/davinci/stats/HostLevelIngestionStats.java`
- `services/venice-server/src/main/java/com/linkedin/venice/stats/ServerHttpRequestStats.java`
- `clients/da-vinci-client/src/main/java/com/linkedin/davinci/stats/BlobTransferStats.java`

### Metric Infrastructure
- `internal/venice-client-common/src/main/java/com/linkedin/venice/stats/metrics/MetricEntity.java`
- `internal/venice-client-common/src/main/java/com/linkedin/venice/stats/metrics/MetricType.java`
- `internal/venice-client-common/src/main/java/com/linkedin/venice/stats/dimensions/VeniceMetricsDimensions.java`
- `internal/venice-client-common/src/main/java/com/linkedin/venice/stats/VeniceOpenTelemetryMetricsRepository.java`
