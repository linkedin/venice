package com.linkedin.venice.client.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_METHOD;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.utils.CollectionUtils.setOf;

import com.linkedin.venice.client.stats.BasicClientStats.BasicClientMetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture.MetricEntityExpectation;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.Test;


public class BasicClientMetricEntityTest {
  /**
   * CALL_COUNT/CALL_COUNT_DVC and CALL_TIME/CALL_TIME_DVC intentionally share the same metric name
   * ("call_count" and "call_time") for consistency across TC/FC and DaVinci client types.
   */
  @Test
  public void testMetricEntities() {
    Map<String, Set<BasicClientMetricEntity>> allowedDuplicates = new HashMap<>();
    allowedDuplicates
        .put("call_count", setOf(BasicClientMetricEntity.CALL_COUNT, BasicClientMetricEntity.CALL_COUNT_DVC));
    allowedDuplicates.put("call_time", setOf(BasicClientMetricEntity.CALL_TIME, BasicClientMetricEntity.CALL_TIME_DVC));
    new ModuleMetricEntityTestFixture<>(BasicClientMetricEntity.class, expectedDefinitions(), allowedDuplicates)
        .assertAll();
  }

  private static Map<BasicClientMetricEntity, MetricEntityExpectation> expectedDefinitions() {
    Map<BasicClientMetricEntity, MetricEntityExpectation> map = new HashMap<>();
    map.put(
        BasicClientMetricEntity.CALL_COUNT,
        new MetricEntityExpectation(
            "call_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of all requests during response handling along with response codes",
            setOf(
                VENICE_STORE_NAME,
                VENICE_REQUEST_METHOD,
                HTTP_RESPONSE_STATUS_CODE,
                HTTP_RESPONSE_STATUS_CODE_CATEGORY,
                VENICE_RESPONSE_STATUS_CODE_CATEGORY)));
    map.put(
        BasicClientMetricEntity.CALL_TIME,
        new MetricEntityExpectation(
            "call_time",
            MetricType.HISTOGRAM,
            MetricUnit.MILLISECOND,
            "Latency based on all responses",
            setOf(
                VENICE_STORE_NAME,
                VENICE_REQUEST_METHOD,
                HTTP_RESPONSE_STATUS_CODE,
                HTTP_RESPONSE_STATUS_CODE_CATEGORY,
                VENICE_RESPONSE_STATUS_CODE_CATEGORY)));
    map.put(
        BasicClientMetricEntity.REQUEST_KEY_COUNT,
        new MetricEntityExpectation(
            "request.key_count",
            MetricType.HISTOGRAM,
            MetricUnit.NUMBER,
            "Count of keys for venice client request",
            setOf(VENICE_STORE_NAME, VENICE_REQUEST_METHOD)));
    map.put(
        BasicClientMetricEntity.RESPONSE_KEY_COUNT,
        new MetricEntityExpectation(
            "response.key_count",
            MetricType.HISTOGRAM,
            MetricUnit.NUMBER,
            "Count of keys for venice client response",
            setOf(VENICE_STORE_NAME, VENICE_REQUEST_METHOD)));
    map.put(
        BasicClientMetricEntity.CALL_COUNT_DVC,
        new MetricEntityExpectation(
            "call_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of all DaVinci Client requests",
            setOf(VENICE_STORE_NAME, VENICE_REQUEST_METHOD, VENICE_RESPONSE_STATUS_CODE_CATEGORY)));
    map.put(
        BasicClientMetricEntity.CALL_TIME_DVC,
        new MetricEntityExpectation(
            "call_time",
            MetricType.HISTOGRAM,
            MetricUnit.MILLISECOND,
            "Latency for all DaVinci Client responses",
            setOf(VENICE_STORE_NAME, VENICE_REQUEST_METHOD, VENICE_RESPONSE_STATUS_CODE_CATEGORY)));
    return map;
  }
}
