package com.linkedin.venice.stats.metrics;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;

import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import java.util.HashSet;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.Test;


public class MetricEntityTest {
  @Test
  public void testMetricEntityConstructorWithDimensions() {
    String metricName = "testMetric";
    MetricType metricType = MetricType.COUNTER;
    MetricUnit unit = MetricUnit.MILLISECOND;
    String description = "Test description with dimensions";

    Set<VeniceMetricsDimensions> dimensions = new HashSet<>();
    dimensions.add(VENICE_STORE_NAME);
    dimensions.add(VENICE_CLUSTER_NAME);

    MetricEntity metricEntity = new MetricEntity(metricName, metricType, unit, description, dimensions);

    Assert.assertEquals(metricEntity.getMetricName(), metricName, "Metric name should match");
    Assert.assertEquals(metricEntity.getMetricType(), metricType, "Metric type should match");
    Assert.assertEquals(metricEntity.getUnit(), unit, "Metric unit should match");
    Assert.assertEquals(metricEntity.getDescription(), description, "Description should match");
    Assert.assertNotNull(metricEntity.getDimensionsList(), "Dimensions list should not be null");
    Assert.assertEquals(metricEntity.getDimensionsList(), dimensions, "Dimensions list should match");
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Metric name cannot be empty")
  public void testMetricEntityConstructorWithEmptyName() {
    new MetricEntity("", MetricType.COUNTER, MetricUnit.MILLISECOND, "Empty name test", new HashSet<>());
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Metric description cannot be empty")
  public void testMetricEntityConstructorWithEmptyDescription() {
    new MetricEntity("testMetric", MetricType.COUNTER, MetricUnit.MILLISECOND, "", new HashSet<>());
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Dimensions list cannot be empty")
  public void testMetricEntityConstructorWithEmptyDimensionsList() {
    new MetricEntity(
        "testMetric",
        MetricType.COUNTER,
        MetricUnit.MILLISECOND,
        "test empty dimension list",
        new HashSet<>());
  }
}
