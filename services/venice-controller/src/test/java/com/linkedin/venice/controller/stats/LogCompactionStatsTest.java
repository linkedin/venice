package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.controller.VeniceController.CONTROLLER_SERVICE_METRIC_ENTITIES;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.STORE_REPUSH_TRIGGER_SOURCE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static org.mockito.Mockito.doReturn;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.venice.controller.AbstractTestVeniceParentHelixAdmin;
import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.dimensions.StoreRepushTriggerSource;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.OpenTelemetryDataTestUtils;
import com.linkedin.venice.utils.Utils;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
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
        LogCompactionStats.LogCompactionOtelMetricEntity.STORE_REPUSH_CALL_COUNT.getMetricName(),
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
            LogCompactionStats.LogCompactionOtelMetricEntity.STORE_COMPACTION_TRIGGERED_COUNT.getMetricName(),
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
          LogCompactionStats.LogCompactionOtelMetricEntity.STORE_COMPACTION_TRIGGERED_COUNT.getMetricName(),
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
        LogCompactionStats.LogCompactionOtelMetricEntity.STORE_REPUSH_CALL_COUNT.getMetricName(),
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
        LogCompactionStats.LogCompactionOtelMetricEntity.STORE_REPUSH_CALL_COUNT.getMetricName(),
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
        LogCompactionStats.LogCompactionOtelMetricEntity.STORE_COMPACTION_ELIGIBLE_STATE.getMetricName(),
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
        LogCompactionStats.LogCompactionOtelMetricEntity.STORE_COMPACTION_ELIGIBLE_STATE.getMetricName(),
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
        LogCompactionStats.LogCompactionOtelMetricEntity.STORE_COMPACTION_NOMINATED_COUNT.getMetricName(),
        1,
        expectedAttributes);
  }

  @Test
  public void testControllerTehutiMetricNameEnum() {
    Map<LogCompactionStats.ControllerTehutiMetricNameEnum, String> expectedNames = new HashMap<>();
    expectedNames.put(LogCompactionStats.ControllerTehutiMetricNameEnum.REPUSH_CALL_COUNT, "repush_call_count");
    expectedNames
        .put(LogCompactionStats.ControllerTehutiMetricNameEnum.COMPACTION_ELIGIBLE_STATE, "compaction_eligible_state");
    expectedNames.put(
        LogCompactionStats.ControllerTehutiMetricNameEnum.STORE_NOMINATED_FOR_COMPACTION_COUNT,
        "store_nominated_for_compaction_count");
    expectedNames.put(
        LogCompactionStats.ControllerTehutiMetricNameEnum.STORE_COMPACTION_TRIGGERED_COUNT,
        "store_compaction_triggered_count");

    assertEquals(
        LogCompactionStats.ControllerTehutiMetricNameEnum.values().length,
        expectedNames.size(),
        "New ControllerTehutiMetricNameEnum values were added but not included in this test");

    for (LogCompactionStats.ControllerTehutiMetricNameEnum enumValue: LogCompactionStats.ControllerTehutiMetricNameEnum
        .values()) {
      String expectedName = expectedNames.get(enumValue);
      assertNotNull(expectedName, "No expected metric name for " + enumValue.name());
      assertEquals(enumValue.getMetricName(), expectedName, "Unexpected metric name for " + enumValue.name());
    }
  }

  @Test
  public void testLogCompactionOtelMetricEntity() {
    Map<LogCompactionStats.LogCompactionOtelMetricEntity, MetricEntity> expectedMetrics = new HashMap<>();
    expectedMetrics.put(
        LogCompactionStats.LogCompactionOtelMetricEntity.STORE_REPUSH_CALL_COUNT,
        new MetricEntity(
            "store.repush.call_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of all requests to repush a store",
            Utils.setOf(
                VeniceMetricsDimensions.VENICE_STORE_NAME,
                VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY,
                VeniceMetricsDimensions.VENICE_CLUSTER_NAME,
                VeniceMetricsDimensions.STORE_REPUSH_TRIGGER_SOURCE)));
    expectedMetrics.put(
        LogCompactionStats.LogCompactionOtelMetricEntity.STORE_COMPACTION_NOMINATED_COUNT,
        new MetricEntity(
            "store.compaction.nominated_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of stores nominated for scheduled compaction",
            Utils.setOf(VeniceMetricsDimensions.VENICE_STORE_NAME, VeniceMetricsDimensions.VENICE_CLUSTER_NAME)));
    expectedMetrics.put(
        LogCompactionStats.LogCompactionOtelMetricEntity.STORE_COMPACTION_ELIGIBLE_STATE,
        new MetricEntity(
            "store.compaction.eligible_state",
            MetricType.GAUGE,
            MetricUnit.NUMBER,
            "Track the state from the time a store is nominated for compaction to the time the repush is completed",
            Utils.setOf(VeniceMetricsDimensions.VENICE_STORE_NAME, VeniceMetricsDimensions.VENICE_CLUSTER_NAME)));
    expectedMetrics.put(
        LogCompactionStats.LogCompactionOtelMetricEntity.STORE_COMPACTION_TRIGGERED_COUNT,
        new MetricEntity(
            "store.compaction.triggered_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of log compaction repush triggered for a store after it becomes eligible",
            Utils.setOf(
                VeniceMetricsDimensions.VENICE_STORE_NAME,
                VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY,
                VeniceMetricsDimensions.VENICE_CLUSTER_NAME)));

    assertEquals(
        LogCompactionStats.LogCompactionOtelMetricEntity.values().length,
        expectedMetrics.size(),
        "New LogCompactionOtelMetricEntity values were added but not included in this test");

    for (LogCompactionStats.LogCompactionOtelMetricEntity metric: LogCompactionStats.LogCompactionOtelMetricEntity
        .values()) {
      MetricEntity actual = metric.getMetricEntity();
      MetricEntity expected = expectedMetrics.get(metric);

      assertNotNull(expected, "No expected definition for " + metric.name());
      assertEquals(actual.getMetricName(), expected.getMetricName(), "Unexpected metric name for " + metric.name());
      assertEquals(actual.getMetricType(), expected.getMetricType(), "Unexpected metric type for " + metric.name());
      assertEquals(actual.getUnit(), expected.getUnit(), "Unexpected metric unit for " + metric.name());
      assertEquals(
          actual.getDescription(),
          expected.getDescription(),
          "Unexpected metric description for " + metric.name());
      assertEquals(
          actual.getDimensionsList(),
          expected.getDimensionsList(),
          "Unexpected metric dimensions for " + metric.name());
    }

    // Verify all LogCompactionOtelMetricEntity entries are present in CONTROLLER_SERVICE_METRIC_ENTITIES
    for (MetricEntity expected: expectedMetrics.values()) {
      boolean found = false;
      for (MetricEntity actual: CONTROLLER_SERVICE_METRIC_ENTITIES) {
        if (Objects.equals(actual.getMetricName(), expected.getMetricName())
            && actual.getMetricType() == expected.getMetricType() && actual.getUnit() == expected.getUnit()
            && Objects.equals(actual.getDescription(), expected.getDescription())
            && Objects.equals(actual.getDimensionsList(), expected.getDimensionsList())) {
          found = true;
          break;
        }
      }
      assertTrue(found, "MetricEntity not found in CONTROLLER_SERVICE_METRIC_ENTITIES: " + expected.getMetricName());
    }
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
