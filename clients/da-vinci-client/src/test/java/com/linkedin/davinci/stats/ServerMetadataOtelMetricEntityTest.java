package com.linkedin.davinci.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture.MetricEntityExpectation;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class ServerMetadataOtelMetricEntityTest {
  @Test
  public void testMetricEntities() {
    new ModuleMetricEntityTestFixture<>(ServerMetadataOtelMetricEntity.class, expectedDefinitions()).assertAll();
  }

  private static Map<ServerMetadataOtelMetricEntity, MetricEntityExpectation> expectedDefinitions() {
    Map<ServerMetadataOtelMetricEntity, MetricEntityExpectation> map = new HashMap<>();
    map.put(
        ServerMetadataOtelMetricEntity.METADATA_REQUEST_COUNT,
        new MetricEntityExpectation(
            "metadata.request_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Request-based metadata invocation count by outcome",
            setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_RESPONSE_STATUS_CODE_CATEGORY)));
    return map;
  }
}
