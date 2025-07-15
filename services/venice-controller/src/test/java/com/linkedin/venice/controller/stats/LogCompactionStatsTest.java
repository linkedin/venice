package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.REPUSH_TRIGGER_SOURCE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static org.mockito.Mockito.doReturn;

import com.linkedin.venice.controller.AbstractTestVeniceParentHelixAdmin;
import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.dimensions.RepushStoreTriggerSource;
import com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.utils.OpenTelemetryDataPointTestUtils;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import java.util.Arrays;
import java.util.Collection;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class LogCompactionStatsTest extends AbstractTestVeniceParentHelixAdmin {
  private static final String TEST_METRIC_PREFIX = "log_compaction";
  private static final String TEST_CLUSTER_NAME = AbstractTestVeniceParentHelixAdmin.clusterName;
  private static final String TEST_STORE_NAME = "log-compaction-stats-test-store";
  private InMemoryMetricReader inMemoryMetricReader;

  private LogCompactionStats logCompactionStats;

  @BeforeMethod
  public void setUp() throws Exception {
    // add all the metrics that are used in the test
    Collection<MetricEntity> metricEntities = Arrays.asList(ControllerMetricEntity.REPUSH_CALL_COUNT.getMetricEntity());

    // setup metric reader to validate metric emission
    this.inMemoryMetricReader = InMemoryMetricReader.create();
    VeniceMetricsRepository metricsRepository = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX)
            .setMetricEntities(metricEntities)
            .setEmitOtelMetrics(true)
            .setOtelAdditionalMetricsReader(inMemoryMetricReader)
            .build());

    setupInternalMocks();
    doReturn(true).when(getConfig()).isLogCompactionEnabled(); // enable log compaction to initialise LogCompactionStats
                                                               // in VeniceParentHelixAdmin

    this.logCompactionStats = new LogCompactionStats(metricsRepository, clusterName);
  }

  /**
   * Test case:
     * Trigger source: manual
     * Response: VeniceParentHelixAdmin#repushStore() receives successful RepushJobResponse
   */
  @Test
  public void testEmitRepushStoreCallCountManualSuccessMetric() throws Exception {
    Attributes expectedAttributes = Attributes.builder()
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), TEST_STORE_NAME)
        .put(
            REPUSH_TRIGGER_SOURCE.getDimensionNameInDefaultFormat(),
            RepushStoreTriggerSource.MANUAL.getDimensionValue())
        .put(
            VENICE_RESPONSE_STATUS_CODE_CATEGORY.getDimensionNameInDefaultFormat(),
            VeniceResponseStatusCategory.SUCCESS.getDimensionValue())
        .build();

    // Record metric
    this.logCompactionStats
        .recordRepushStoreCall(TEST_STORE_NAME, RepushStoreTriggerSource.MANUAL, VeniceResponseStatusCategory.SUCCESS);

    // test validation
    validateMetricEmission(ControllerMetricEntity.REPUSH_CALL_COUNT.getMetricName(), 1, expectedAttributes);
  }

  /**
   * Test case:
     * Trigger source: manual
     * Response: VeniceParentHelixAdmin#repushStore() receives RepushJobResponse with error flag set to true
   */
  @Test
  public void testEmitRepushStoreCallCountManualFailWithErrorResponseMetric() throws Exception {
    Attributes expectedAttributes = Attributes.builder()
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), TEST_STORE_NAME)
        .put(
            REPUSH_TRIGGER_SOURCE.getDimensionNameInDefaultFormat(),
            RepushStoreTriggerSource.MANUAL.getDimensionValue())
        .put(
            VENICE_RESPONSE_STATUS_CODE_CATEGORY.getDimensionNameInDefaultFormat(),
            VeniceResponseStatusCategory.FAIL.getDimensionValue())
        .build();

    // Record metric
    this.logCompactionStats
        .recordRepushStoreCall(TEST_STORE_NAME, RepushStoreTriggerSource.MANUAL, VeniceResponseStatusCategory.FAIL);

    // test validation
    validateMetricEmission(ControllerMetricEntity.REPUSH_CALL_COUNT.getMetricName(), 1, expectedAttributes);
  }

  /**
   * Test case:
     * Trigger source: scheduled
     * Response: VeniceParentHelixAdmin#repushStore() receives successful RepushJobResponse
   *
   * Note: Only the happy test case is implemented for scheduled repush because the behaviour in the other two cases
   * (error and exception) are covered by the corresponding manual repush test cases.
   */
  @Test
  public void testEmitRepushStoreCallCountScheduledSuccessMetric() throws Exception {
    Attributes expectedAttributes = Attributes.builder()
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), TEST_STORE_NAME)
        .put(
            REPUSH_TRIGGER_SOURCE.getDimensionNameInDefaultFormat(),
            RepushStoreTriggerSource.SCHEDULED.getDimensionValue())
        .put(
            VENICE_RESPONSE_STATUS_CODE_CATEGORY.getDimensionNameInDefaultFormat(),
            VeniceResponseStatusCategory.SUCCESS.getDimensionValue())
        .build();

    // Record metric
    this.logCompactionStats.recordRepushStoreCall(
        TEST_STORE_NAME,
        RepushStoreTriggerSource.SCHEDULED,
        VeniceResponseStatusCategory.SUCCESS);

    // test validation
    validateMetricEmission(ControllerMetricEntity.REPUSH_CALL_COUNT.getMetricName(), 1, expectedAttributes);
  }

  // TODO PRANAV
  // @Test
  // public void testEmitCompactionEnabled() throws Exception {
  // Attributes expectedAttributes = Attributes.builder()
  // .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
  // .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), TEST_STORE_NAME)
  // .build();
  //
  // // Record metric
  // this.logCompactionStats.setCompactionEligible(TEST_STORE_NAME);
  //
  // // test validation
  // validateMetricEmission(ControllerMetricEntity.COMPACTION_ELIGIBLE_STATE.getMetricName(), 1, expectedAttributes);
  // }
  //
  // @Test
  // public void testEmitCompactionComplete() throws Exception {
  // Attributes expectedAttributes = Attributes.builder()
  // .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
  // .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), TEST_STORE_NAME)
  // .build();
  //
  // // Record metric
  // this.logCompactionStats.setCompactionComplete(TEST_STORE_NAME);
  //
  // // test validation
  // validateMetricEmission(ControllerMetricEntity.COMPACTION_ELIGIBLE_STATE.getMetricName(), 1, expectedAttributes);
  // }

  @Test
  public void testEmitStoreNominatedForCompaction() throws Exception {
    Attributes expectedAttributes = Attributes.builder()
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), TEST_STORE_NAME)
        .build();

    // Record metric
    this.logCompactionStats.recordStoreNominatedForCompactionCount(TEST_STORE_NAME);

    // test validation
    validateMetricEmission(
        ControllerMetricEntity.STORE_NOMINATED_FOR_COMPACTION_COUNT.getMetricName(),
        1,
        expectedAttributes);
  }

  private void validateMetricEmission(String metricName, int expectedMetricValue, Attributes expectedAttributes) {
    OpenTelemetryDataPointTestUtils.validateLongPointData(
        inMemoryMetricReader,
        expectedMetricValue,
        expectedAttributes,
        metricName,
        TEST_METRIC_PREFIX);
  }
}
