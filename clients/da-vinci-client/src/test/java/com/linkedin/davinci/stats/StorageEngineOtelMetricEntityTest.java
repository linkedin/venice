package com.linkedin.davinci.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_RECORD_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_VERSION_ROLE;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture.MetricEntityExpectation;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class StorageEngineOtelMetricEntityTest {
  @Test
  public void testMetricEntities() {
    new ModuleMetricEntityTestFixture<>(StorageEngineOtelMetricEntity.class, expectedDefinitions()).assertAll();
  }

  private static Map<StorageEngineOtelMetricEntity, MetricEntityExpectation> expectedDefinitions() {
    Map<StorageEngineOtelMetricEntity, MetricEntityExpectation> map = new HashMap<>();
    map.put(
        StorageEngineOtelMetricEntity.DISK_USAGE,
        new MetricEntityExpectation(
            "ingestion.disk.used",
            MetricType.ASYNC_GAUGE,
            MetricUnit.BYTES,
            "Disk usage in bytes by record type (data or replication metadata)",
            setOf(VENICE_CLUSTER_NAME, VENICE_STORE_NAME, VENICE_VERSION_ROLE, VENICE_RECORD_TYPE)));
    map.put(
        StorageEngineOtelMetricEntity.ROCKSDB_OPEN_FAILURE_COUNT,
        new MetricEntityExpectation(
            "rocksdb.open.failure_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of RocksDB open failures; VERSION_ROLE reflects the version's role at failure time, not its current role",
            setOf(VENICE_CLUSTER_NAME, VENICE_STORE_NAME, VENICE_VERSION_ROLE)));
    map.put(
        StorageEngineOtelMetricEntity.KEY_COUNT_ESTIMATE,
        new MetricEntityExpectation(
            "rocksdb.key.estimated_count",
            MetricType.ASYNC_GAUGE,
            MetricUnit.NUMBER,
            "Estimated key count in the storage engine",
            setOf(VENICE_CLUSTER_NAME, VENICE_STORE_NAME, VENICE_VERSION_ROLE)));
    return map;
  }
}
