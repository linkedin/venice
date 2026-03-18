package com.linkedin.venice.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_QUOTA_REQUEST_OUTCOME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_VERSION_ROLE;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.davinci.stats.ServerReadQuotaOtelMetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture.MetricEntityExpectation;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class ServerReadQuotaOtelMetricEntityTest {
  @Test
  public void testMetricEntities() {
    new ModuleMetricEntityTestFixture<>(ServerReadQuotaOtelMetricEntity.class, expectedDefinitions()).assertAll();
  }

  private static Map<ServerReadQuotaOtelMetricEntity, MetricEntityExpectation> expectedDefinitions() {
    Map<ServerReadQuotaOtelMetricEntity, MetricEntityExpectation> map = new HashMap<>();
    map.put(
        ServerReadQuotaOtelMetricEntity.READ_QUOTA_REQUEST_COUNT,
        new MetricEntityExpectation(
            "read.quota.request.count",
            MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES,
            MetricUnit.NUMBER,
            "Count of read quota requests per outcome and version role",
            setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE, VENICE_QUOTA_REQUEST_OUTCOME)));
    map.put(
        ServerReadQuotaOtelMetricEntity.READ_QUOTA_KEY_COUNT,
        new MetricEntityExpectation(
            "read.quota.key.count",
            MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES,
            MetricUnit.NUMBER,
            "Count of read quota keys (RCU) per outcome and version role",
            setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE, VENICE_QUOTA_REQUEST_OUTCOME)));
    map.put(
        ServerReadQuotaOtelMetricEntity.READ_QUOTA_USAGE_RATIO,
        new MetricEntityExpectation(
            "read.quota.usage_ratio",
            MetricType.ASYNC_DOUBLE_GAUGE,
            MetricUnit.RATIO,
            "Ratio of read quota used, based on requested keys per second relative to the node's quota responsibility",
            setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME)));
    return map;
  }
}
