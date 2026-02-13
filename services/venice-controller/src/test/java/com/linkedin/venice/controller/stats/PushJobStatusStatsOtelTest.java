package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.controller.VeniceController.CONTROLLER_SERVICE_METRIC_ENTITIES;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_PUSH_JOB_STATUS;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_PUSH_JOB_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;

import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.dimensions.VenicePushJobStatus;
import com.linkedin.venice.stats.dimensions.VenicePushType;
import com.linkedin.venice.utils.OpenTelemetryDataTestUtils;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
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
        ControllerMetricEntity.PUSH_JOB_COUNT.getMetricName(),
        1,
        Attributes.builder()
            .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
            .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), TEST_STORE_NAME)
            .put(VENICE_PUSH_JOB_TYPE.getDimensionNameInDefaultFormat(), VenicePushType.BATCH.getDimensionValue())
            .put(
                VENICE_PUSH_JOB_STATUS.getDimensionNameInDefaultFormat(),
                VenicePushJobStatus.SUCCESS.getDimensionValue())
            .build());
  }

  @Test
  public void testRecordBatchPushFailureDueToUserError() {
    stats.recordBatchPushFailureDueToUserErrorSensor(TEST_STORE_NAME);
    validateCounter(
        ControllerMetricEntity.PUSH_JOB_COUNT.getMetricName(),
        1,
        Attributes.builder()
            .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
            .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), TEST_STORE_NAME)
            .put(VENICE_PUSH_JOB_TYPE.getDimensionNameInDefaultFormat(), VenicePushType.BATCH.getDimensionValue())
            .put(
                VENICE_PUSH_JOB_STATUS.getDimensionNameInDefaultFormat(),
                VenicePushJobStatus.USER_ERROR.getDimensionValue())
            .build());
  }

  @Test
  public void testRecordBatchPushFailureNotDueToUserError() {
    stats.recordBatchPushFailureNotDueToUserErrorSensor(TEST_STORE_NAME);
    validateCounter(
        ControllerMetricEntity.PUSH_JOB_COUNT.getMetricName(),
        1,
        Attributes.builder()
            .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
            .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), TEST_STORE_NAME)
            .put(VENICE_PUSH_JOB_TYPE.getDimensionNameInDefaultFormat(), VenicePushType.BATCH.getDimensionValue())
            .put(
                VENICE_PUSH_JOB_STATUS.getDimensionNameInDefaultFormat(),
                VenicePushJobStatus.SYSTEM_ERROR.getDimensionValue())
            .build());
  }

  @Test
  public void testRecordIncrementalPushSuccess() {
    stats.recordIncrementalPushSuccessSensor(TEST_STORE_NAME);
    validateCounter(
        ControllerMetricEntity.PUSH_JOB_COUNT.getMetricName(),
        1,
        Attributes.builder()
            .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
            .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), TEST_STORE_NAME)
            .put(VENICE_PUSH_JOB_TYPE.getDimensionNameInDefaultFormat(), VenicePushType.INCREMENTAL.getDimensionValue())
            .put(
                VENICE_PUSH_JOB_STATUS.getDimensionNameInDefaultFormat(),
                VenicePushJobStatus.SUCCESS.getDimensionValue())
            .build());
  }

  @Test
  public void testRecordIncrementalPushFailureDueToUserError() {
    stats.recordIncrementalPushFailureDueToUserErrorSensor(TEST_STORE_NAME);
    validateCounter(
        ControllerMetricEntity.PUSH_JOB_COUNT.getMetricName(),
        1,
        Attributes.builder()
            .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
            .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), TEST_STORE_NAME)
            .put(VENICE_PUSH_JOB_TYPE.getDimensionNameInDefaultFormat(), VenicePushType.INCREMENTAL.getDimensionValue())
            .put(
                VENICE_PUSH_JOB_STATUS.getDimensionNameInDefaultFormat(),
                VenicePushJobStatus.USER_ERROR.getDimensionValue())
            .build());
  }

  @Test
  public void testRecordIncrementalPushFailureNotDueToUserError() {
    stats.recordIncrementalPushFailureNotDueToUserErrorSensor(TEST_STORE_NAME);
    validateCounter(
        ControllerMetricEntity.PUSH_JOB_COUNT.getMetricName(),
        1,
        Attributes.builder()
            .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
            .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), TEST_STORE_NAME)
            .put(VENICE_PUSH_JOB_TYPE.getDimensionNameInDefaultFormat(), VenicePushType.INCREMENTAL.getDimensionValue())
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
        ControllerMetricEntity.PUSH_JOB_COUNT.getMetricName(),
        1,
        Attributes.builder()
            .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
            .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), "store-a")
            .put(VENICE_PUSH_JOB_TYPE.getDimensionNameInDefaultFormat(), VenicePushType.BATCH.getDimensionValue())
            .put(
                VENICE_PUSH_JOB_STATUS.getDimensionNameInDefaultFormat(),
                VenicePushJobStatus.SUCCESS.getDimensionValue())
            .build());

    validateCounter(
        ControllerMetricEntity.PUSH_JOB_COUNT.getMetricName(),
        1,
        Attributes.builder()
            .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
            .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), "store-b")
            .put(VENICE_PUSH_JOB_TYPE.getDimensionNameInDefaultFormat(), VenicePushType.BATCH.getDimensionValue())
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

  private void validateCounter(String metricName, long expectedValue, Attributes expectedAttributes) {
    OpenTelemetryDataTestUtils.validateLongPointDataFromCounter(
        inMemoryMetricReader,
        expectedValue,
        expectedAttributes,
        metricName,
        TEST_METRIC_PREFIX);
  }
}
