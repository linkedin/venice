package com.linkedin.davinci.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.utils.Utils.setOf;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import org.testng.annotations.Test;


public class ServerMetadataOtelMetricEntityTest {
  @Test
  public void testMetricEntityCount() {
    assertEquals(ServerMetadataOtelMetricEntity.values().length, 1, "Expected 1 metric entity");
  }

  @Test
  public void testMetricEntityDefinitions() {
    MetricEntity entity = ServerMetadataOtelMetricEntity.METADATA_REQUEST_COUNT.getMetricEntity();

    assertNotNull(entity);
    assertEquals(entity.getMetricName(), "metadata.request_count");
    assertEquals(entity.getMetricType(), MetricType.COUNTER);
    assertEquals(entity.getUnit(), MetricUnit.NUMBER);
    assertEquals(entity.getDescription(), "Request-based metadata invocation count by outcome");
    assertEquals(
        entity.getDimensionsList(),
        setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_RESPONSE_STATUS_CODE_CATEGORY));
  }
}
