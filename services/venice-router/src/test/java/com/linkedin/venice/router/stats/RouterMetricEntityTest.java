package com.linkedin.venice.router.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_MESSAGE_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_KEY_COUNT_BUCKET;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_METHOD;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_RETRY_ABORT_REASON;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_RETRY_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture.assertNoDuplicateMetricNamesAcrossEnums;
import static com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture.metricEntitiesEqual;
import static com.linkedin.venice.utils.Utils.setOf;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.router.RouterServer;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture.MetricEntityExpectation;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class RouterMetricEntityTest {
  private static Map<RouterMetricEntity, MetricEntityExpectation> expectedDefinitions() {
    Map<RouterMetricEntity, MetricEntityExpectation> map = new HashMap<>();
    map.put(
        RouterMetricEntity.CALL_COUNT,
        new MetricEntityExpectation(
            "call_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of all requests during response handling along with response codes",
            setOf(
                VENICE_STORE_NAME,
                VENICE_CLUSTER_NAME,
                VENICE_REQUEST_METHOD,
                HTTP_RESPONSE_STATUS_CODE,
                HTTP_RESPONSE_STATUS_CODE_CATEGORY,
                VENICE_RESPONSE_STATUS_CODE_CATEGORY)));
    map.put(
        RouterMetricEntity.CALL_TIME,
        new MetricEntityExpectation(
            "call_time",
            MetricType.HISTOGRAM,
            MetricUnit.MILLISECOND,
            "Latency based on all responses",
            setOf(
                VENICE_STORE_NAME,
                VENICE_CLUSTER_NAME,
                VENICE_REQUEST_METHOD,
                HTTP_RESPONSE_STATUS_CODE,
                HTTP_RESPONSE_STATUS_CODE_CATEGORY,
                VENICE_RESPONSE_STATUS_CODE_CATEGORY,
                VENICE_REQUEST_KEY_COUNT_BUCKET)));
    map.put(
        RouterMetricEntity.CALL_SIZE,
        new MetricEntityExpectation(
            "call_size",
            MetricType.HISTOGRAM,
            MetricUnit.BYTES,
            "Size of request and response in bytes",
            setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_REQUEST_METHOD, VENICE_MESSAGE_TYPE)));
    map.put(
        RouterMetricEntity.KEY_SIZE,
        new MetricEntityExpectation(
            "key_size",
            MetricType.HISTOGRAM,
            MetricUnit.BYTES,
            "Size of keys in bytes",
            setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_REQUEST_METHOD)));
    map.put(
        RouterMetricEntity.KEY_COUNT,
        new MetricEntityExpectation(
            "key_count",
            MetricType.HISTOGRAM,
            MetricUnit.NUMBER,
            "Count of keys during response handling along with response codes",
            setOf(
                VENICE_STORE_NAME,
                VENICE_CLUSTER_NAME,
                VENICE_REQUEST_METHOD,
                HTTP_RESPONSE_STATUS_CODE,
                HTTP_RESPONSE_STATUS_CODE_CATEGORY,
                VENICE_RESPONSE_STATUS_CODE_CATEGORY)));
    map.put(
        RouterMetricEntity.RETRY_COUNT,
        new MetricEntityExpectation(
            "retry_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of retries triggered",
            setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_REQUEST_METHOD, VENICE_REQUEST_RETRY_TYPE)));
    map.put(
        RouterMetricEntity.ALLOWED_RETRY_COUNT,
        new MetricEntityExpectation(
            "allowed_retry_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of allowed retry requests",
            setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_REQUEST_METHOD)));
    map.put(
        RouterMetricEntity.DISALLOWED_RETRY_COUNT,
        new MetricEntityExpectation(
            "disallowed_retry_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of disallowed retry requests",
            setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_REQUEST_METHOD)));
    map.put(
        RouterMetricEntity.ABORTED_RETRY_COUNT,
        new MetricEntityExpectation(
            "aborted_retry_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of aborted retry requests",
            setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_REQUEST_METHOD, VENICE_REQUEST_RETRY_ABORT_REASON)));
    map.put(
        RouterMetricEntity.RETRY_DELAY,
        new MetricEntityExpectation(
            "retry_delay",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.MILLISECOND,
            "Retry delay time",
            setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_REQUEST_METHOD)));
    return map;
  }

  @Test
  public void testMetricEntities() {
    new ModuleMetricEntityTestFixture<>(RouterMetricEntity.class, expectedDefinitions()).assertAll();
  }

  /**
   * Verifies that no two enum constants across all router metric entity enums share the same
   * metric name. Uses {@link RouterServer#getMetricEntityEnumClasses()} as the single source of
   * truth. Scans raw enum constants to catch silent deduplication by
   * {@link ModuleMetricEntityInterface#getUniqueMetricEntities}.
   */
  @Test
  public void testNoDuplicateMetricNamesAcrossRouterEnums() {
    assertNoDuplicateMetricNamesAcrossEnums(RouterServer.getMetricEntityEnumClasses());
  }

  @Test
  public void testRouterServiceMetricEntitiesRegistration() {
    // Build the full expected set from the same source of truth as production
    Collection<MetricEntity> expectedMetricEntities =
        ModuleMetricEntityInterface.getUniqueMetricEntities(RouterServer.getMetricEntityEnumClasses());

    // Assert size
    assertEquals(
        RouterServer.ROUTER_SERVICE_METRIC_ENTITIES.size(),
        expectedMetricEntities.size(),
        "Unexpected size of ROUTER_SERVICE_METRIC_ENTITIES");

    // Assert contents
    for (MetricEntity actual: RouterServer.ROUTER_SERVICE_METRIC_ENTITIES) {
      boolean found = false;
      for (MetricEntity expected: expectedMetricEntities) {
        if (metricEntitiesEqual(actual, expected)) {
          found = true;
          break;
        }
      }
      assertTrue(found, "Unexpected MetricEntity found: " + actual.getMetricName());
    }
  }

}
