package com.linkedin.venice.fastclient.stats;

import static com.linkedin.venice.fastclient.stats.ClusterMetricEntity.INSTANCE_ERROR_COUNT;
import static com.linkedin.venice.fastclient.stats.ClusterMetricEntity.STORE_VERSION_CURRENT;
import static com.linkedin.venice.fastclient.stats.ClusterMetricEntity.STORE_VERSION_UPDATE_FAILURE_COUNT;
import static com.linkedin.venice.stats.ClientType.FAST_CLIENT;
import static com.linkedin.venice.stats.VeniceMetricsRepository.getVeniceMetricsRepository;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_INSTANCE_ERROR_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.utils.OpenTelemetryDataTestUtils.validateHistogramPointData;
import static com.linkedin.venice.utils.OpenTelemetryDataTestUtils.validateLongPointDataFromCounter;
import static com.linkedin.venice.utils.OpenTelemetryDataTestUtils.validateLongPointDataFromGauge;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.stats.VeniceMetricsRepository;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.Metric;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class ClusterStatsTest {
  @Test
  public void testMultipleVersionUpdateFailures() {
    String storeName = "test_store";
    InMemoryMetricReader inMemoryMetricReader = InMemoryMetricReader.create();
    VeniceMetricsRepository veniceMetricsRepository = getVeniceMetricsRepository(
        FAST_CLIENT,
        Arrays.asList(STORE_VERSION_UPDATE_FAILURE_COUNT.getMetricEntity()),
        true,
        inMemoryMetricReader);

    ClusterStats stats = new ClusterStats(veniceMetricsRepository, storeName);

    // Record multiple version update failures
    int failureCount = 5;
    for (int i = 0; i < failureCount; i++) {
      stats.recordVersionUpdateFailure();
    }

    // Validate Tehuti metrics
    Map<String, ? extends Metric> tehutiMetrics = veniceMetricsRepository.metrics();
    String expectedTehutiMetricName = String.format(".%s--version_update_failure.OccurrenceRate", storeName);
    assertNotNull(tehutiMetrics.get(expectedTehutiMetricName), "Tehuti metric should exist");
    assertTrue(tehutiMetrics.get(expectedTehutiMetricName).value() > 0.0, "Tehuti metric should have recorded values");

    // Validate OpenTelemetry metrics - counter should accumulate
    Attributes expectedAttributes =
        Attributes.builder().put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), storeName).build();

    validateLongPointDataFromCounter(
        inMemoryMetricReader,
        (long) failureCount,
        expectedAttributes,
        STORE_VERSION_UPDATE_FAILURE_COUNT.getMetricEntity().getMetricName(),
        FAST_CLIENT.getMetricsPrefix());
  }

  @Test
  public void testInstanceErrorCounts() {
    String storeName = "test_store";
    InMemoryMetricReader inMemoryMetricReader = InMemoryMetricReader.create();
    VeniceMetricsRepository veniceMetricsRepository = getVeniceMetricsRepository(
        FAST_CLIENT,
        Arrays.asList(INSTANCE_ERROR_COUNT.getMetricEntity()),
        true,
        inMemoryMetricReader);

    ClusterStats stats = new ClusterStats(veniceMetricsRepository, storeName);

    // Test blocked instance count
    int blockedCount = 3;
    stats.recordBlockedInstanceCount(blockedCount);

    // Test unhealthy instance count
    int unhealthyCount = 5;
    stats.recordUnhealthyInstanceCount(unhealthyCount);

    // Test overloaded instance count
    int overloadedCount = 2;
    stats.recordOverloadedInstanceCount(overloadedCount);

    // Validate Tehuti metrics still work
    Map<String, ? extends Metric> tehutiMetrics = veniceMetricsRepository.metrics();
    String expectedBlockedMetricName = String.format(".%s--blocked_instance_count.Avg", storeName);
    String expectedUnhealthyMetricName = String.format(".%s--unhealthy_instance_count.Avg", storeName);
    String expectedOverloadedMetricName = String.format(".%s--overloaded_instance_count.Avg", storeName);

    assertNotNull(tehutiMetrics.get(expectedBlockedMetricName), "Blocked instance Tehuti metric should exist");
    assertNotNull(tehutiMetrics.get(expectedUnhealthyMetricName), "Unhealthy instance Tehuti metric should exist");
    assertNotNull(tehutiMetrics.get(expectedOverloadedMetricName), "Overloaded instance Tehuti metric should exist");

    // Validate OpenTelemetry metrics for blocked instances
    Attributes blockedAttributes = Attributes.builder()
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), storeName)
        .put(VENICE_INSTANCE_ERROR_TYPE.getDimensionNameInDefaultFormat(), "blocked")
        .build();

    validateHistogramPointData(
        inMemoryMetricReader,
        (double) blockedCount, // min
        (double) blockedCount, // max
        1L, // count
        (double) blockedCount, // sum
        blockedAttributes,
        INSTANCE_ERROR_COUNT.getMetricEntity().getMetricName(),
        FAST_CLIENT.getMetricsPrefix());

    // Validate OpenTelemetry metrics for unhealthy instances
    Attributes unhealthyAttributes = Attributes.builder()
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), storeName)
        .put(VENICE_INSTANCE_ERROR_TYPE.getDimensionNameInDefaultFormat(), "unhealthy")
        .build();

    validateHistogramPointData(
        inMemoryMetricReader,
        (double) unhealthyCount, // min
        (double) unhealthyCount, // max
        1L, // count
        (double) unhealthyCount, // sum
        unhealthyAttributes,
        INSTANCE_ERROR_COUNT.getMetricEntity().getMetricName(),
        FAST_CLIENT.getMetricsPrefix());

    // Validate OpenTelemetry metrics for overloaded instances
    Attributes overloadedAttributes = Attributes.builder()
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), storeName)
        .put(VENICE_INSTANCE_ERROR_TYPE.getDimensionNameInDefaultFormat(), "overloaded")
        .build();

    validateHistogramPointData(
        inMemoryMetricReader,
        (double) overloadedCount, // min
        (double) overloadedCount, // max
        1L, // count
        (double) overloadedCount, // sum
        overloadedAttributes,
        INSTANCE_ERROR_COUNT.getMetricEntity().getMetricName(),
        FAST_CLIENT.getMetricsPrefix());
  }

  @Test
  public void testCurrentVersionNumber() {
    String storeName = "test_store";
    InMemoryMetricReader inMemoryMetricReader = InMemoryMetricReader.create();
    VeniceMetricsRepository veniceMetricsRepository = getVeniceMetricsRepository(
        FAST_CLIENT,
        Arrays.asList(STORE_VERSION_CURRENT.getMetricEntity()),
        true,
        inMemoryMetricReader);

    ClusterStats stats = new ClusterStats(veniceMetricsRepository, storeName);

    // Test initial version (should be -1)
    int initialVersion = -1;

    // Validate Tehuti metrics for initial version
    Map<String, ? extends Metric> tehutiMetrics = veniceMetricsRepository.metrics();
    String expectedTehutiMetricName = String.format(".%s--current_version.Gauge", storeName);
    assertNotNull(tehutiMetrics.get(expectedTehutiMetricName), "Current version Tehuti metric should exist");
    assertTrue(
        tehutiMetrics.get(expectedTehutiMetricName).value() == initialVersion,
        "Initial version should be " + initialVersion);

    // Validate OpenTelemetry metrics for initial version
    Attributes expectedAttributes =
        Attributes.builder().put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), storeName).build();

    validateLongPointDataFromGauge(
        inMemoryMetricReader,
        initialVersion,
        expectedAttributes,
        STORE_VERSION_CURRENT.getMetricEntity().getMetricName(),
        FAST_CLIENT.getMetricsPrefix());

    // Test updating to a new version
    int newVersion = 5;
    stats.updateCurrentVersion(newVersion);

    // Validate Tehuti metrics after version update
    tehutiMetrics = veniceMetricsRepository.metrics();
    assertTrue(
        tehutiMetrics.get(expectedTehutiMetricName).value() == newVersion,
        "Updated version should be " + newVersion);

    // Validate OpenTelemetry metrics after version update
    validateLongPointDataFromGauge(
        inMemoryMetricReader,
        newVersion,
        expectedAttributes,
        STORE_VERSION_CURRENT.getMetricEntity().getMetricName(),
        FAST_CLIENT.getMetricsPrefix());
  }

  @Test
  public void testClusterTehutiMetricNameEnum() {
    Map<ClusterStats.ClusterTehutiMetricName, String> expectedNames = new HashMap<>();
    expectedNames.put(ClusterStats.ClusterTehutiMetricName.VERSION_UPDATE_FAILURE, "version_update_failure");
    expectedNames.put(ClusterStats.ClusterTehutiMetricName.CURRENT_VERSION, "current_version");
    expectedNames.put(ClusterStats.ClusterTehutiMetricName.BLOCKED_INSTANCE_COUNT, "blocked_instance_count");
    expectedNames.put(ClusterStats.ClusterTehutiMetricName.UNHEALTHY_INSTANCE_COUNT, "unhealthy_instance_count");
    expectedNames.put(ClusterStats.ClusterTehutiMetricName.OVERLOADED_INSTANCE_COUNT, "overloaded_instance_count");

    assertEquals(
        ClusterStats.ClusterTehutiMetricName.values().length,
        expectedNames.size(),
        "New ClusterTehutiMetricName values were added but not included in this test");

    for (ClusterStats.ClusterTehutiMetricName enumValue: ClusterStats.ClusterTehutiMetricName.values()) {
      String expectedName = expectedNames.get(enumValue);
      assertNotNull(expectedName, "No expected metric name for " + enumValue.name());
      assertEquals(enumValue.getMetricName(), expectedName, "Unexpected metric name for " + enumValue.name());
    }
  }
}
