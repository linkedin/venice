package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.controller.VeniceController.CONTROLLER_SERVICE_METRIC_ENTITIES;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.STORE_REPUSH_TRIGGER_SOURCE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static org.mockito.Mockito.doReturn;
import static org.testng.Assert.fail;

import com.linkedin.venice.controller.AbstractTestVeniceParentHelixAdmin;
import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.dimensions.StoreRepushTriggerSource;
import com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.OpenTelemetryDataTestUtils;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
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
    // setup metric reader to validate metric emission
    this.inMemoryMetricReader = InMemoryMetricReader.create();
    VeniceMetricsRepository metricsRepository = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX)
            .setMetricEntities(CONTROLLER_SERVICE_METRIC_ENTITIES)
            .setEmitOtelMetrics(true)
            .setOtelAdditionalMetricsReader(inMemoryMetricReader)
            .build());

    setupInternalMocks();
    // enable log compaction to initialise LogCompactionStats in VeniceParentHelixAdmin
    doReturn(true).when(getConfig()).isLogCompactionEnabled();

    this.logCompactionStats = new LogCompactionStats(metricsRepository, clusterName);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testEmitRepushStoreCallCountManualSuccessMetric(boolean isManualTrigger) {
    StoreRepushTriggerSource triggerSource =
        isManualTrigger ? StoreRepushTriggerSource.MANUAL : StoreRepushTriggerSource.SCHEDULED_FOR_LOG_COMPACTION;
    Attributes expectedAttributesForRepushCount = Attributes.builder()
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), TEST_STORE_NAME)
        .put(STORE_REPUSH_TRIGGER_SOURCE.getDimensionNameInDefaultFormat(), triggerSource.getDimensionValue())
        .put(
            VENICE_RESPONSE_STATUS_CODE_CATEGORY.getDimensionNameInDefaultFormat(),
            VeniceResponseStatusCategory.SUCCESS.getDimensionValue())
        .build();

    // Record metric
    this.logCompactionStats.recordRepushStoreCall(TEST_STORE_NAME, triggerSource, VeniceResponseStatusCategory.SUCCESS);

    // test validation
    validateLongPointFromDataFromSum(
        ControllerMetricEntity.STORE_REPUSH_CALL_COUNT.getMetricName(),
        1,
        expectedAttributesForRepushCount);

    Attributes expectedAttributesForCompactionTriggered = Attributes.builder()
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), TEST_STORE_NAME)
        .put(
            VENICE_RESPONSE_STATUS_CODE_CATEGORY.getDimensionNameInDefaultFormat(),
            VeniceResponseStatusCategory.SUCCESS.getDimensionValue())
        .build();

    if (isManualTrigger) {
      try {
        validateLongPointFromDataFromSum(
            ControllerMetricEntity.STORE_COMPACTION_TRIGGERED_COUNT.getMetricName(),
            1,
            expectedAttributesForCompactionTriggered);
        fail("Compaction triggered metric should NOT be emitted for manual trigger");
      } catch (AssertionError e) {
        // For manual trigger, the compaction triggered metric should NOT be emitted
        if (!e.getMessage().contains("MetricData should not be null")) {
          throw e;
        }
      }
    } else {
      // For scheduled trigger, the compaction triggered metric should also be emitted
      validateLongPointFromDataFromSum(
          ControllerMetricEntity.STORE_COMPACTION_TRIGGERED_COUNT.getMetricName(),
          1,
          expectedAttributesForCompactionTriggered);
    }
  }

  @Test
  public void testEmitRepushStoreCallCountManualFailWithErrorResponseMetric() {
    Attributes expectedAttributes = Attributes.builder()
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), TEST_STORE_NAME)
        .put(
            STORE_REPUSH_TRIGGER_SOURCE.getDimensionNameInDefaultFormat(),
            StoreRepushTriggerSource.MANUAL.getDimensionValue())
        .put(
            VENICE_RESPONSE_STATUS_CODE_CATEGORY.getDimensionNameInDefaultFormat(),
            VeniceResponseStatusCategory.FAIL.getDimensionValue())
        .build();

    // Record metric
    this.logCompactionStats
        .recordRepushStoreCall(TEST_STORE_NAME, StoreRepushTriggerSource.MANUAL, VeniceResponseStatusCategory.FAIL);

    // test validation
    validateLongPointFromDataFromSum(
        ControllerMetricEntity.STORE_REPUSH_CALL_COUNT.getMetricName(),
        1,
        expectedAttributes);
  }

  @Test
  public void testEmitRepushStoreCallCountScheduledSuccessMetric() {
    Attributes expectedAttributes = Attributes.builder()
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), TEST_STORE_NAME)
        .put(
            STORE_REPUSH_TRIGGER_SOURCE.getDimensionNameInDefaultFormat(),
            StoreRepushTriggerSource.SCHEDULED_FOR_LOG_COMPACTION.getDimensionValue())
        .put(
            VENICE_RESPONSE_STATUS_CODE_CATEGORY.getDimensionNameInDefaultFormat(),
            VeniceResponseStatusCategory.SUCCESS.getDimensionValue())
        .build();

    // Record metric
    this.logCompactionStats.recordRepushStoreCall(
        TEST_STORE_NAME,
        StoreRepushTriggerSource.SCHEDULED_FOR_LOG_COMPACTION,
        VeniceResponseStatusCategory.SUCCESS);

    // test validation
    validateLongPointFromDataFromSum(
        ControllerMetricEntity.STORE_REPUSH_CALL_COUNT.getMetricName(),
        1,
        expectedAttributes);
  }

  @Test
  public void testEmitCompactionEligible() {
    Attributes expectedAttributes = Attributes.builder()
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), TEST_STORE_NAME)
        .build();

    // Record metric
    this.logCompactionStats.setCompactionEligible(TEST_STORE_NAME);

    // test validation
    validateLongPointFromDataFromGauge(
        ControllerMetricEntity.STORE_COMPACTION_ELIGIBLE_STATE.getMetricName(),
        1,
        expectedAttributes);
  }

  @Test
  public void testEmitCompactionComplete() {
    Attributes expectedAttributes = Attributes.builder()
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), TEST_STORE_NAME)
        .build();

    // Record metric
    this.logCompactionStats.setCompactionComplete(TEST_STORE_NAME);

    // test validation
    validateLongPointFromDataFromGauge(
        ControllerMetricEntity.STORE_COMPACTION_ELIGIBLE_STATE.getMetricName(),
        0,
        expectedAttributes);
  }

  @Test
  public void testEmitStoreNominatedForCompaction() {
    Attributes expectedAttributes = Attributes.builder()
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), TEST_STORE_NAME)
        .build();

    // Record metric
    this.logCompactionStats.recordStoreNominatedForCompactionCount(TEST_STORE_NAME);

    // test validation
    validateLongPointFromDataFromSum(
        ControllerMetricEntity.STORE_COMPACTION_NOMINATED_COUNT.getMetricName(),
        1,
        expectedAttributes);
  }

  private void validateLongPointFromDataFromSum(
      String metricName,
      int expectedMetricValue,
      Attributes expectedAttributes) {
    OpenTelemetryDataTestUtils.validateLongPointDataFromCounter(
        inMemoryMetricReader,
        expectedMetricValue,
        expectedAttributes,
        metricName,
        TEST_METRIC_PREFIX);
  }

  private void validateLongPointFromDataFromGauge(
      String metricName,
      long expectedMetricValue,
      Attributes expectedAttributes) {
    OpenTelemetryDataTestUtils.validateLongPointDataFromGauge(
        inMemoryMetricReader,
        expectedMetricValue,
        expectedAttributes,
        metricName,
        TEST_METRIC_PREFIX);
  }
}
