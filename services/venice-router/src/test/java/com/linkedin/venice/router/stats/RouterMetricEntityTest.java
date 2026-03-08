package com.linkedin.venice.router.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_MESSAGE_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_METHOD;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_RETRY_ABORT_REASON;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_RETRY_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.utils.Utils.setOf;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.router.RouterServer;
import com.linkedin.venice.stats.ThreadPoolOtelMetricEntity;
import com.linkedin.venice.stats.metrics.AbstractModuleMetricEntityTest;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.testng.annotations.Test;


public class RouterMetricEntityTest extends AbstractModuleMetricEntityTest<RouterMetricEntity> {
  public RouterMetricEntityTest() {
    super(RouterMetricEntity.class);
  }

  @Override
  protected Map<RouterMetricEntity, MetricEntityExpectation> expectedDefinitions() {
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
                VENICE_RESPONSE_STATUS_CODE_CATEGORY)));
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

  /**
   * Verifies that no two enum constants across all router metric entity enums share the same
   * metric name. Duplicates would cause silent deduplication in {@code getUniqueMetricEntities}.
   */
  @Test
  public void testNoDuplicateMetricNamesAcrossRouterEnums() {
    Set<String> allNames = new HashSet<>();
    for (MetricEntity entity: RouterServer.ROUTER_SERVICE_METRIC_ENTITIES) {
      String name = entity.getMetricName();
      assertTrue(allNames.add(name), "Duplicate metric name found across router enums: " + name);
    }
  }

  @Test
  public void testRouterServiceMetricEntitiesRegistration() {
    // Build the full expected set: RouterMetricEntity + shared module entities (ThreadPoolOtelMetricEntity)
    Collection<MetricEntity> expectedMetricEntities =
        ModuleMetricEntityInterface.getUniqueMetricEntities(RouterMetricEntity.class, ThreadPoolOtelMetricEntity.class);

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

  private boolean metricEntitiesEqual(MetricEntity actual, MetricEntity expected) {
    return Objects.equals(actual.getMetricName(), expected.getMetricName())
        && actual.getMetricType() == expected.getMetricType() && actual.getUnit() == expected.getUnit()
        && Objects.equals(actual.getDescription(), expected.getDescription())
        && Objects.equals(actual.getDimensionsList(), expected.getDimensionsList());
  }
}
