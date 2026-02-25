package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.controller.VeniceController.CONTROLLER_SERVICE_METRIC_ENTITIES;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_PUSH_JOB_STATUS;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_PUSH_JOB_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.meta.Version.PushType;
import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.dimensions.VenicePushJobStatus;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.utils.OpenTelemetryDataTestUtils;
import com.linkedin.venice.utils.Utils;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class PushJobStatusStatsOtelTest {
  private static final String TEST_METRIC_PREFIX = "controller";
  private static final String TEST_CLUSTER_NAME = "test-cluster";
  private static final String TEST_STORE_NAME = "test-store";
  private InMemoryMetricReader inMemoryMetricReader;
  private PushJobStatusStats stats;

  @BeforeMethod
  public void setUp() {
    this.inMemoryMetricReader = InMemoryMetricReader.create();
    VeniceMetricsRepository metricsRepository = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX)
            .setMetricEntities(CONTROLLER_SERVICE_METRIC_ENTITIES)
            .setEmitOtelMetrics(true)
            .setOtelAdditionalMetricsReader(inMemoryMetricReader)
            .build());

    stats = new PushJobStatusStats(metricsRepository, TEST_CLUSTER_NAME);
  }

  @Test
  public void testRecordBatchPushSuccess() {
    stats.recordBatchPushSuccessSensor(TEST_STORE_NAME);
    validateCounter(
        PushJobStatusStats.PushJobOtelMetricEntity.PUSH_JOB_COUNT.getMetricName(),
        1,
        Attributes.builder()
            .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
            .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), TEST_STORE_NAME)
            .put(VENICE_PUSH_JOB_TYPE.getDimensionNameInDefaultFormat(), PushType.BATCH.getDimensionValue())
            .put(
                VENICE_PUSH_JOB_STATUS.getDimensionNameInDefaultFormat(),
                VenicePushJobStatus.SUCCESS.getDimensionValue())
            .build());
  }

  @Test
  public void testRecordBatchPushFailureDueToUserError() {
    stats.recordBatchPushFailureDueToUserErrorSensor(TEST_STORE_NAME);
    validateCounter(
        PushJobStatusStats.PushJobOtelMetricEntity.PUSH_JOB_COUNT.getMetricName(),
        1,
        Attributes.builder()
            .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
            .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), TEST_STORE_NAME)
            .put(VENICE_PUSH_JOB_TYPE.getDimensionNameInDefaultFormat(), PushType.BATCH.getDimensionValue())
            .put(
                VENICE_PUSH_JOB_STATUS.getDimensionNameInDefaultFormat(),
                VenicePushJobStatus.USER_ERROR.getDimensionValue())
            .build());
  }

  @Test
  public void testRecordBatchPushFailureNotDueToUserError() {
    stats.recordBatchPushFailureNotDueToUserErrorSensor(TEST_STORE_NAME);
    validateCounter(
        PushJobStatusStats.PushJobOtelMetricEntity.PUSH_JOB_COUNT.getMetricName(),
        1,
        Attributes.builder()
            .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
            .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), TEST_STORE_NAME)
            .put(VENICE_PUSH_JOB_TYPE.getDimensionNameInDefaultFormat(), PushType.BATCH.getDimensionValue())
            .put(
                VENICE_PUSH_JOB_STATUS.getDimensionNameInDefaultFormat(),
                VenicePushJobStatus.SYSTEM_ERROR.getDimensionValue())
            .build());
  }

  @Test
  public void testRecordIncrementalPushSuccess() {
    stats.recordIncrementalPushSuccessSensor(TEST_STORE_NAME);
    validateCounter(
        PushJobStatusStats.PushJobOtelMetricEntity.PUSH_JOB_COUNT.getMetricName(),
        1,
        Attributes.builder()
            .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
            .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), TEST_STORE_NAME)
            .put(VENICE_PUSH_JOB_TYPE.getDimensionNameInDefaultFormat(), PushType.INCREMENTAL.getDimensionValue())
            .put(
                VENICE_PUSH_JOB_STATUS.getDimensionNameInDefaultFormat(),
                VenicePushJobStatus.SUCCESS.getDimensionValue())
            .build());
  }

  @Test
  public void testRecordIncrementalPushFailureDueToUserError() {
    stats.recordIncrementalPushFailureDueToUserErrorSensor(TEST_STORE_NAME);
    validateCounter(
        PushJobStatusStats.PushJobOtelMetricEntity.PUSH_JOB_COUNT.getMetricName(),
        1,
        Attributes.builder()
            .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
            .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), TEST_STORE_NAME)
            .put(VENICE_PUSH_JOB_TYPE.getDimensionNameInDefaultFormat(), PushType.INCREMENTAL.getDimensionValue())
            .put(
                VENICE_PUSH_JOB_STATUS.getDimensionNameInDefaultFormat(),
                VenicePushJobStatus.USER_ERROR.getDimensionValue())
            .build());
  }

  @Test
  public void testRecordIncrementalPushFailureNotDueToUserError() {
    stats.recordIncrementalPushFailureNotDueToUserErrorSensor(TEST_STORE_NAME);
    validateCounter(
        PushJobStatusStats.PushJobOtelMetricEntity.PUSH_JOB_COUNT.getMetricName(),
        1,
        Attributes.builder()
            .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
            .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), TEST_STORE_NAME)
            .put(VENICE_PUSH_JOB_TYPE.getDimensionNameInDefaultFormat(), PushType.INCREMENTAL.getDimensionValue())
            .put(
                VENICE_PUSH_JOB_STATUS.getDimensionNameInDefaultFormat(),
                VenicePushJobStatus.SYSTEM_ERROR.getDimensionValue())
            .build());
  }

  @Test
  public void testDifferentStoresRecordSeparately() {
    stats.recordBatchPushSuccessSensor("store-a");
    stats.recordBatchPushSuccessSensor("store-b");

    validateCounter(
        PushJobStatusStats.PushJobOtelMetricEntity.PUSH_JOB_COUNT.getMetricName(),
        1,
        Attributes.builder()
            .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
            .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), "store-a")
            .put(VENICE_PUSH_JOB_TYPE.getDimensionNameInDefaultFormat(), PushType.BATCH.getDimensionValue())
            .put(
                VENICE_PUSH_JOB_STATUS.getDimensionNameInDefaultFormat(),
                VenicePushJobStatus.SUCCESS.getDimensionValue())
            .build());

    validateCounter(
        PushJobStatusStats.PushJobOtelMetricEntity.PUSH_JOB_COUNT.getMetricName(),
        1,
        Attributes.builder()
            .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
            .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), "store-b")
            .put(VENICE_PUSH_JOB_TYPE.getDimensionNameInDefaultFormat(), PushType.BATCH.getDimensionValue())
            .put(
                VENICE_PUSH_JOB_STATUS.getDimensionNameInDefaultFormat(),
                VenicePushJobStatus.SUCCESS.getDimensionValue())
            .build());
  }

  @Test
  public void testNoNpeWhenOtelDisabled() {
    VeniceMetricsRepository disabledRepo = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX).setEmitOtelMetrics(false).build());
    PushJobStatusStats disabledStats = new PushJobStatusStats(disabledRepo, TEST_CLUSTER_NAME);

    disabledStats.recordBatchPushSuccessSensor(TEST_STORE_NAME);
    disabledStats.recordBatchPushFailureDueToUserErrorSensor(TEST_STORE_NAME);
    disabledStats.recordBatchPushFailureNotDueToUserErrorSensor(TEST_STORE_NAME);
    disabledStats.recordIncrementalPushSuccessSensor(TEST_STORE_NAME);
    disabledStats.recordIncrementalPushFailureDueToUserErrorSensor(TEST_STORE_NAME);
    disabledStats.recordIncrementalPushFailureNotDueToUserErrorSensor(TEST_STORE_NAME);
  }

  @Test
  public void testPushJobTehutiMetricNameEnum() {
    Map<PushJobStatusStats.PushJobTehutiMetricNameEnum, String> expectedNames = new HashMap<>();
    expectedNames.put(PushJobStatusStats.PushJobTehutiMetricNameEnum.BATCH_PUSH_JOB_SUCCESS, "batch_push_job_success");
    expectedNames.put(
        PushJobStatusStats.PushJobTehutiMetricNameEnum.BATCH_PUSH_JOB_FAILED_USER_ERROR,
        "batch_push_job_failed_user_error");
    expectedNames.put(
        PushJobStatusStats.PushJobTehutiMetricNameEnum.BATCH_PUSH_JOB_FAILED_NON_USER_ERROR,
        "batch_push_job_failed_non_user_error");
    expectedNames.put(
        PushJobStatusStats.PushJobTehutiMetricNameEnum.INCREMENTAL_PUSH_JOB_SUCCESS,
        "incremental_push_job_success");
    expectedNames.put(
        PushJobStatusStats.PushJobTehutiMetricNameEnum.INCREMENTAL_PUSH_JOB_FAILED_USER_ERROR,
        "incremental_push_job_failed_user_error");
    expectedNames.put(
        PushJobStatusStats.PushJobTehutiMetricNameEnum.INCREMENTAL_PUSH_JOB_FAILED_NON_USER_ERROR,
        "incremental_push_job_failed_non_user_error");

    assertEquals(
        PushJobStatusStats.PushJobTehutiMetricNameEnum.values().length,
        expectedNames.size(),
        "New PushJobTehutiMetricNameEnum values were added but not included in this test");

    for (PushJobStatusStats.PushJobTehutiMetricNameEnum enumValue: PushJobStatusStats.PushJobTehutiMetricNameEnum
        .values()) {
      String expectedName = expectedNames.get(enumValue);
      assertNotNull(expectedName, "No expected metric name for " + enumValue.name());
      assertEquals(enumValue.getMetricName(), expectedName, "Unexpected metric name for " + enumValue.name());
    }
  }

  @Test
  public void testPushJobOtelMetricEntity() {
    Map<PushJobStatusStats.PushJobOtelMetricEntity, MetricEntity> expectedMetrics = new HashMap<>();
    expectedMetrics.put(
        PushJobStatusStats.PushJobOtelMetricEntity.PUSH_JOB_COUNT,
        new MetricEntity(
            "push_job.count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Push job completions, differentiated by push type and status",
            Utils.setOf(
                VeniceMetricsDimensions.VENICE_CLUSTER_NAME,
                VeniceMetricsDimensions.VENICE_STORE_NAME,
                VeniceMetricsDimensions.VENICE_PUSH_JOB_TYPE,
                VeniceMetricsDimensions.VENICE_PUSH_JOB_STATUS)));

    assertEquals(
        PushJobStatusStats.PushJobOtelMetricEntity.values().length,
        expectedMetrics.size(),
        "New PushJobOtelMetricEntity values were added but not included in this test");

    for (PushJobStatusStats.PushJobOtelMetricEntity metric: PushJobStatusStats.PushJobOtelMetricEntity.values()) {
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

    // Verify all PushJobOtelMetricEntity entries are present in CONTROLLER_SERVICE_METRIC_ENTITIES
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

  private void validateCounter(String metricName, long expectedValue, Attributes expectedAttributes) {
    OpenTelemetryDataTestUtils.validateLongPointDataFromCounter(
        inMemoryMetricReader,
        expectedValue,
        expectedAttributes,
        metricName,
        TEST_METRIC_PREFIX);
  }
}
