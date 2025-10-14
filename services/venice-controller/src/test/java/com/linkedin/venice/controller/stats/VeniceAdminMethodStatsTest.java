package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_PARENT_ADMIN_METHOD;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_PARENT_ADMIN_METHOD_STEP;

import com.linkedin.venice.controller.AbstractTestVeniceParentHelixAdmin;
import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.utils.OpenTelemetryDataPointTestUtils;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import java.util.Arrays;
import java.util.Collection;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class VeniceAdminMethodStatsTest extends AbstractTestVeniceParentHelixAdmin {
  private static final String TEST_METRIC_PREFIX = "venice_admin_method";
  private static final String TEST_CLUSTER_NAME = AbstractTestVeniceParentHelixAdmin.clusterName;
  private InMemoryMetricReader inMemoryMetricReader;
  private VeniceAdminMethodStats veniceAdminMethodStats;

  @BeforeMethod
  public void setUp() throws Exception {
    // add all the metrics that are used in the test
    Collection<MetricEntity> metricEntities =
        Arrays.asList(AdminBaseMetricEntity.PARENT_ADMIN_CALL_TIME.getMetricEntity());

    // setup metric reader to validate metric emission
    this.inMemoryMetricReader = InMemoryMetricReader.create();
    VeniceMetricsRepository metricsRepository = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX)
            .setMetricEntities(metricEntities)
            .setEmitOtelMetrics(true)
            .setOtelAdditionalMetricsReader(inMemoryMetricReader)
            .build());

    setupInternalMocks();

    this.veniceAdminMethodStats = new VeniceAdminMethodStats(metricsRepository, TEST_CLUSTER_NAME);
  }

  @Test
  public void testRecordMethodLatency() {
    long testCallTime = 1000L;
    this.veniceAdminMethodStats.recordParentAdminMethodStepLatency(
        VeniceAdminMethod.INCREMENT_VERSION_IDEMPOTENT,
        VeniceAdminMethodStep.ADD_VERSION_AND_TOPIC_ONLY,
        testCallTime);

    // Test validation
    validateExponentialHistogramPointData(
        AdminBaseMetricEntity.PARENT_ADMIN_CALL_TIME.getMetricName(),
        testCallTime,
        testCallTime,
        1,
        testCallTime,
        Attributes.builder()
            .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
            .put(
                VENICE_PARENT_ADMIN_METHOD.getDimensionNameInDefaultFormat(),
                VeniceAdminMethod.INCREMENT_VERSION_IDEMPOTENT.getDimensionValue())

            .put(
                VENICE_PARENT_ADMIN_METHOD_STEP.getDimensionNameInDefaultFormat(),
                VeniceAdminMethodStep.ADD_VERSION_AND_TOPIC_ONLY.getDimensionValue())
            .build());
  }

  private void validateExponentialHistogramPointData(
      String metricName,
      double expectedMin,
      double expectedMax,
      long expectedCount,
      double expectedSum,
      Attributes expectedAttributes) {
    OpenTelemetryDataPointTestUtils.validateExponentialHistogramPointData(
        inMemoryMetricReader,
        expectedMin,
        expectedMax,
        expectedCount,
        expectedSum,
        expectedAttributes,
        metricName,
        TEST_METRIC_PREFIX);

  }
}
