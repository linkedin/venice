package com.linkedin.davinci.stats.ingestion.heartbeat;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REGION_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REPLICA_STATE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REPLICA_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_VERSION_ROLE;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.stats.metrics.AbstractModuleMetricEntityTest;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import java.util.HashMap;
import java.util.Map;


public class HeartbeatOtelMetricEntityTest extends AbstractModuleMetricEntityTest<HeartbeatOtelMetricEntity> {
  public HeartbeatOtelMetricEntityTest() {
    super(HeartbeatOtelMetricEntity.class);
  }

  @Override
  protected Map<HeartbeatOtelMetricEntity, MetricEntityExpectation> expectedDefinitions() {
    Map<HeartbeatOtelMetricEntity, MetricEntityExpectation> map = new HashMap<>();
    map.put(
        HeartbeatOtelMetricEntity.INGESTION_HEARTBEAT_DELAY,
        new MetricEntityExpectation(
            "ingestion.replication.heartbeat.delay",
            MetricType.HISTOGRAM,
            MetricUnit.MILLISECOND,
            "Nearline ingestion replication lag measured via heartbeat messages",
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
