package com.linkedin.davinci.stats.ingestion.heartbeat;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REGION_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REPLICA_STATE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REPLICA_TYPE;
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


public class RecordLevelDelayOtelMetricEntityTest {
  @Test
  public void testMetricEntities() {
    new ModuleMetricEntityTestFixture<>(RecordLevelDelayOtelMetricEntity.class, expectedDefinitions()).assertAll();
  }

  private static Map<RecordLevelDelayOtelMetricEntity, MetricEntityExpectation> expectedDefinitions() {
    Map<RecordLevelDelayOtelMetricEntity, MetricEntityExpectation> map = new HashMap<>();
    map.put(
        RecordLevelDelayOtelMetricEntity.INGESTION_RECORD_DELAY,
        new MetricEntityExpectation(
            "ingestion.replication.record.delay",
            MetricType.HISTOGRAM,
            MetricUnit.MILLISECOND,
            "Nearline ingestion record-level replication lag",
            setOf(
                VENICE_STORE_NAME,
                VENICE_CLUSTER_NAME,
                VENICE_REGION_NAME,
                VENICE_VERSION_ROLE,
                VENICE_REPLICA_TYPE,
                VENICE_REPLICA_STATE)));
    return map;
  }
}
