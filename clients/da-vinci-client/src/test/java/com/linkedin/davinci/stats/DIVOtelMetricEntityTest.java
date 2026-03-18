package com.linkedin.davinci.stats;

import static com.linkedin.davinci.stats.DIVOtelMetricEntity.BENIGN_PRODUCER_FAILURE_COUNT;
import static com.linkedin.davinci.stats.DIVOtelMetricEntity.MESSAGE_COUNT;
import static com.linkedin.davinci.stats.DIVOtelMetricEntity.OFFSET_REWIND_COUNT;
import static com.linkedin.davinci.stats.DIVOtelMetricEntity.PRODUCER_FAILURE_COUNT;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_DIV_RESULT;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_DIV_SEVERITY;
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


public class DIVOtelMetricEntityTest {
  @Test
  public void testMetricEntities() {
    new ModuleMetricEntityTestFixture<>(DIVOtelMetricEntity.class, expectedDefinitions()).assertAll();
  }

  private static Map<DIVOtelMetricEntity, MetricEntityExpectation> expectedDefinitions() {
    Map<DIVOtelMetricEntity, MetricEntityExpectation> map = new HashMap<>();
    map.put(
        MESSAGE_COUNT,
        new MetricEntityExpectation(
            "ingestion.div.message.count",
            MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES,
            MetricUnit.NUMBER,
            "Count of DIV message validation results",
            setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE, VENICE_DIV_RESULT)));
    map.put(
        OFFSET_REWIND_COUNT,
        new MetricEntityExpectation(
            "ingestion.div.offset.rewind.count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of leader offset rewind events",
            setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE, VENICE_DIV_SEVERITY)));
    map.put(
        PRODUCER_FAILURE_COUNT,
        new MetricEntityExpectation(
            "ingestion.div.producer.failure.count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of leader producer failures",
            setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)));
    map.put(
        BENIGN_PRODUCER_FAILURE_COUNT,
        new MetricEntityExpectation(
            "ingestion.div.producer.failure.benign_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of benign leader producer failures",
            setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE)));
    return map;
  }
}
