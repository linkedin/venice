package com.linkedin.davinci.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_RECORD_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_VERSION_ROLE;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface;
import java.util.Set;


/**
 * OTel metric entities for storage engine statistics.
 *
 * <p>Consolidates 4 Tehuti AsyncGauge sensors into 3 OTel metrics:
 * <ul>
 *   <li>{@code disk_usage_in_bytes} + {@code rmd_disk_usage_in_bytes} consolidated into
 *       {@link #DISK_USAGE} with {@code RECORD_TYPE} dimension (DATA vs REPLICATION_METADATA)</li>
 *   <li>{@code rocksdb_open_failure_count} maps to {@link #ROCKSDB_OPEN_FAILURE_COUNT} (COUNTER)</li>
 *   <li>{@code rocksdb_key_count_estimate} maps to {@link #KEY_COUNT_ESTIMATE} (ASYNC_GAUGE)</li>
 * </ul>
 */
public enum StorageEngineOtelMetricEntity implements ModuleMetricEntityInterface {
  DISK_USAGE(
      "ingestion.disk.used", MetricType.ASYNC_GAUGE, MetricUnit.BYTES,
      "Disk usage in bytes by record type (data or replication metadata)",
      setOf(VENICE_CLUSTER_NAME, VENICE_STORE_NAME, VENICE_VERSION_ROLE, VENICE_RECORD_TYPE)
  ),

  ROCKSDB_OPEN_FAILURE_COUNT(
      "rocksdb.open.failure_count", MetricType.COUNTER, MetricUnit.NUMBER,
      "Count of RocksDB open failures; VERSION_ROLE reflects the version's role at failure time, not its current role",
      setOf(VENICE_CLUSTER_NAME, VENICE_STORE_NAME, VENICE_VERSION_ROLE)
  ),

  KEY_COUNT_ESTIMATE(
      "rocksdb.key.estimated_count", MetricType.ASYNC_GAUGE, MetricUnit.NUMBER,
      "Estimated key count in the storage engine", setOf(VENICE_CLUSTER_NAME, VENICE_STORE_NAME, VENICE_VERSION_ROLE)
  );

  private final MetricEntity metricEntity;

  StorageEngineOtelMetricEntity(
      String name,
      MetricType metricType,
      MetricUnit unit,
      String description,
      Set<VeniceMetricsDimensions> dimensions) {
    this.metricEntity = new MetricEntity(name, metricType, unit, description, dimensions);
  }

  @Override
  public MetricEntity getMetricEntity() {
    return metricEntity;
  }
}
