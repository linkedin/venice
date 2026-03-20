package com.linkedin.davinci.stats;

import static com.linkedin.davinci.stats.ServerConnectionOtelMetricEntity.CONNECTION_ACTIVE_COUNT;
import static com.linkedin.davinci.stats.ServerConnectionOtelMetricEntity.CONNECTION_REQUEST_COUNT;
import static com.linkedin.davinci.stats.ServerConnectionOtelMetricEntity.CONNECTION_SETUP_TIME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CONNECTION_SOURCE;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture.MetricEntityExpectation;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class ServerConnectionOtelMetricEntityTest {
  @Test
  public void testMetricEntities() {
    new ModuleMetricEntityTestFixture<>(ServerConnectionOtelMetricEntity.class, expectedDefinitions()).assertAll();
  }

  private static Map<ServerConnectionOtelMetricEntity, MetricEntityExpectation> expectedDefinitions() {
    Map<ServerConnectionOtelMetricEntity, MetricEntityExpectation> map = new HashMap<>();
    map.put(
        CONNECTION_ACTIVE_COUNT,
        new MetricEntityExpectation(
            "connection.active_count",
            MetricType.UP_DOWN_COUNTER,
            MetricUnit.NUMBER,
            "Active connection count by source (router or client)",
            setOf(VENICE_CLUSTER_NAME, VENICE_CONNECTION_SOURCE)));
    map.put(
        CONNECTION_REQUEST_COUNT,
        new MetricEntityExpectation(
            "connection.request_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Connection establishment requests by source",
            setOf(VENICE_CLUSTER_NAME, VENICE_CONNECTION_SOURCE)));
    map.put(
        CONNECTION_SETUP_TIME,
        new MetricEntityExpectation(
            "connection.setup_time",
            MetricType.HISTOGRAM,
            MetricUnit.MILLISECOND,
            "SSL handshake setup latency from channel init to completion",
            setOf(VENICE_CLUSTER_NAME, VENICE_CONNECTION_SOURCE)));
    return map;
  }
}
