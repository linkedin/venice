package com.linkedin.davinci.stats;

import static com.linkedin.davinci.stats.DaVinciRecordTransformerOtelMetricEntity.RECORD_TRANSFORMER_ERROR_COUNT;
import static com.linkedin.davinci.stats.DaVinciRecordTransformerOtelMetricEntity.RECORD_TRANSFORMER_LATENCY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_RECORD_TRANSFORMER_OPERATION;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture.MetricEntityExpectation;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class DaVinciRecordTransformerOtelMetricEntityTest {
  @Test
  public void testMetricEntities() {
    new ModuleMetricEntityTestFixture<>(DaVinciRecordTransformerOtelMetricEntity.class, expectedDefinitions())
        .assertAll();
  }

  private static Map<DaVinciRecordTransformerOtelMetricEntity, MetricEntityExpectation> expectedDefinitions() {
    Map<DaVinciRecordTransformerOtelMetricEntity, MetricEntityExpectation> map = new HashMap<>();
    map.put(
        RECORD_TRANSFORMER_LATENCY,
        new MetricEntityExpectation(
            "record_transformer.latency",
            MetricType.HISTOGRAM,
            MetricUnit.MILLISECOND,
            "DaVinci record transformer operation latency",
            setOf(VENICE_CLUSTER_NAME, VENICE_STORE_NAME, VENICE_RECORD_TRANSFORMER_OPERATION)));
    map.put(
        RECORD_TRANSFORMER_ERROR_COUNT,
        new MetricEntityExpectation(
            "record_transformer.error_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "DaVinci record transformer operation error count",
            setOf(VENICE_CLUSTER_NAME, VENICE_STORE_NAME, VENICE_RECORD_TRANSFORMER_OPERATION)));
    return map;
  }
}
