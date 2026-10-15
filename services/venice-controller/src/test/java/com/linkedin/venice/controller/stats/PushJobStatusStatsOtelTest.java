package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.controller.VeniceController.CONTROLLER_SERVICE_METRIC_ENTITIES;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_PUSH_JOB_DATA_WRITER_SINK;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_PUSH_JOB_DURATION_BUCKET;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_PUSH_JOB_EXECUTION_STATE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_PUSH_JOB_STATUS;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_PUSH_JOB_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REGION_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.stats.dimensions.VenicePushJobDurationBucket.AT_OR_OVER_SLA;
import static com.linkedin.venice.stats.dimensions.VenicePushJobDurationBucket.UNDER_SLA;
import static com.linkedin.venice.status.PushJobDetailsStatus.COMPLETED;
import static com.linkedin.venice.status.PushJobDetailsStatus.ERROR;
import static com.linkedin.venice.status.PushJobDetailsStatus.KILLED;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.controller.stats.PushJobStatusStats.PushJobTehutiMetricNameEnum;
import com.linkedin.venice.meta.Version.PushType;
import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.dimensions.VenicePushJobDataWriterSink;
import com.linkedin.venice.stats.dimensions.VenicePushJobDurationBucket;
import com.linkedin.venice.stats.dimensions.VenicePushJobStatus;
import com.linkedin.venice.status.PushJobDetailsStatus;
import com.linkedin.venice.utils.OpenTelemetryDataTestUtils;
import com.linkedin.venice.utils.metrics.MetricsRepositoryUtils;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.metrics.data.ExponentialHistogramPointData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class PushJobStatusStatsOtelTest {
  private static final String TEST_METRIC_PREFIX = "controller";
  private static final String TEST_CLUSTER_NAME = "test-cluster";
  private static final String TEST_STORE_NAME = "test-store";
  private InMemoryMetricReader inMemoryMetricReader;
  private VeniceMetricsRepository metricsRepository;
  private PushJobStatusStats stats;

  @BeforeMethod
  public void setUp() {
    this.inMemoryMetricReader = InMemoryMetricReader.create();
    metricsRepository = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX)
            .setMetricEntities(CONTROLLER_SERVICE_METRIC_ENTITIES)
            .setEmitOtelMetrics(true)
            .setOtelAdditionalMetricsReader(inMemoryMetricReader)
            .setTehutiMetricConfig(MetricsRepositoryUtils.createDefaultSingleThreadedMetricConfig())
            .build());

    stats = new PushJobStatusStats(metricsRepository, TEST_CLUSTER_NAME);
  }

  @DataProvider
  public Object[][] durationBuckets() {
    return new Object[][] { { UNDER_SLA }, { AT_OR_OVER_SLA } };
  }

  @DataProvider
  public Object[][] failedStatesAndDurationBuckets() {
    return new Object[][] { { ERROR, UNDER_SLA }, { ERROR, AT_OR_OVER_SLA }, { KILLED, UNDER_SLA },
        { KILLED, AT_OR_OVER_SLA } };
  }

  @Test(dataProvider = "durationBuckets")
  public void testRecordBatchPushSuccess(VenicePushJobDurationBucket durationBucket) {
    stats.recordBatchPushSuccessSensor(TEST_STORE_NAME, COMPLETED, durationBucket);
    validateCounter(
        PushJobStatusStats.PushJobOtelMetricEntity.PUSH_JOB_COUNT.getMetricName(),
        1,
        pushJobAttributes(TEST_STORE_NAME, PushType.BATCH, VenicePushJobStatus.SUCCESS, COMPLETED, durationBucket));
  }

  @Test(dataProvider = "failedStatesAndDurationBuckets")
  public void testRecordBatchPushFailureDueToUserError(
      PushJobDetailsStatus executionState,
      VenicePushJobDurationBucket durationBucket) {
    stats.recordBatchPushFailureDueToUserErrorSensor(TEST_STORE_NAME, executionState, durationBucket);
    validateCounter(
        PushJobStatusStats.PushJobOtelMetricEntity.PUSH_JOB_COUNT.getMetricName(),
        1,
        pushJobAttributes(
            TEST_STORE_NAME,
            PushType.BATCH,
            VenicePushJobStatus.USER_ERROR,
            executionState,
            durationBucket));
  }

  @Test(dataProvider = "failedStatesAndDurationBuckets")
  public void testRecordBatchPushFailureNotDueToUserError(
      PushJobDetailsStatus executionState,
      VenicePushJobDurationBucket durationBucket) {
    stats.recordBatchPushFailureNotDueToUserErrorSensor(TEST_STORE_NAME, executionState, durationBucket);
    validateCounter(
        PushJobStatusStats.PushJobOtelMetricEntity.PUSH_JOB_COUNT.getMetricName(),
        1,
        pushJobAttributes(
            TEST_STORE_NAME,
            PushType.BATCH,
            VenicePushJobStatus.SYSTEM_ERROR,
            executionState,
            durationBucket));
  }

  @Test(dataProvider = "durationBuckets")
  public void testRecordIncrementalPushSuccess(VenicePushJobDurationBucket durationBucket) {
    stats.recordIncrementalPushSuccessSensor(TEST_STORE_NAME, COMPLETED, durationBucket);
    validateCounter(
        PushJobStatusStats.PushJobOtelMetricEntity.PUSH_JOB_COUNT.getMetricName(),
        1,
        pushJobAttributes(
            TEST_STORE_NAME,
            PushType.INCREMENTAL,
            VenicePushJobStatus.SUCCESS,
            COMPLETED,
            durationBucket));
  }

  @Test(dataProvider = "failedStatesAndDurationBuckets")
  public void testRecordIncrementalPushFailureDueToUserError(
      PushJobDetailsStatus executionState,
      VenicePushJobDurationBucket durationBucket) {
    stats.recordIncrementalPushFailureDueToUserErrorSensor(TEST_STORE_NAME, executionState, durationBucket);
    validateCounter(
        PushJobStatusStats.PushJobOtelMetricEntity.PUSH_JOB_COUNT.getMetricName(),
        1,
        pushJobAttributes(
            TEST_STORE_NAME,
            PushType.INCREMENTAL,
            VenicePushJobStatus.USER_ERROR,
            executionState,
            durationBucket));
  }

  @Test(dataProvider = "failedStatesAndDurationBuckets")
  public void testRecordIncrementalPushFailureNotDueToUserError(
      PushJobDetailsStatus executionState,
      VenicePushJobDurationBucket durationBucket) {
    stats.recordIncrementalPushFailureNotDueToUserErrorSensor(TEST_STORE_NAME, executionState, durationBucket);
    validateCounter(
        PushJobStatusStats.PushJobOtelMetricEntity.PUSH_JOB_COUNT.getMetricName(),
        1,
        pushJobAttributes(
            TEST_STORE_NAME,
            PushType.INCREMENTAL,
            VenicePushJobStatus.SYSTEM_ERROR,
            executionState,
            durationBucket));
  }

  @Test
  public void testDifferentStoresRecordSeparately() {
    stats.recordBatchPushSuccessSensor("store-a", COMPLETED, UNDER_SLA);
    stats.recordBatchPushSuccessSensor("store-b", COMPLETED, UNDER_SLA);

    validateCounter(
        PushJobStatusStats.PushJobOtelMetricEntity.PUSH_JOB_COUNT.getMetricName(),
        1,
        pushJobAttributes("store-a", PushType.BATCH, VenicePushJobStatus.SUCCESS, COMPLETED, UNDER_SLA));

    validateCounter(
        PushJobStatusStats.PushJobOtelMetricEntity.PUSH_JOB_COUNT.getMetricName(),
        1,
        pushJobAttributes("store-b", PushType.BATCH, VenicePushJobStatus.SUCCESS, COMPLETED, UNDER_SLA));
  }

  @Test
  public void testExecutionStatesAndDurationBucketsRecordSeparately() {
    stats.recordBatchPushFailureNotDueToUserErrorSensor(TEST_STORE_NAME, ERROR, UNDER_SLA);
    stats.recordBatchPushFailureNotDueToUserErrorSensor(TEST_STORE_NAME, KILLED, UNDER_SLA);
    stats.recordBatchPushFailureNotDueToUserErrorSensor(TEST_STORE_NAME, KILLED, UNDER_SLA);
    stats.recordBatchPushFailureNotDueToUserErrorSensor(TEST_STORE_NAME, KILLED, AT_OR_OVER_SLA);

    validateCounter(
        PushJobStatusStats.PushJobOtelMetricEntity.PUSH_JOB_COUNT.getMetricName(),
        1,
        pushJobAttributes(TEST_STORE_NAME, PushType.BATCH, VenicePushJobStatus.SYSTEM_ERROR, ERROR, UNDER_SLA));
    validateCounter(
        PushJobStatusStats.PushJobOtelMetricEntity.PUSH_JOB_COUNT.getMetricName(),
        2,
        pushJobAttributes(TEST_STORE_NAME, PushType.BATCH, VenicePushJobStatus.SYSTEM_ERROR, KILLED, UNDER_SLA));
    validateCounter(
        PushJobStatusStats.PushJobOtelMetricEntity.PUSH_JOB_COUNT.getMetricName(),
        1,
        pushJobAttributes(TEST_STORE_NAME, PushType.BATCH, VenicePushJobStatus.SYSTEM_ERROR, KILLED, AT_OR_OVER_SLA));
    validateTehutiCount(metricsRepository, PushJobTehutiMetricNameEnum.BATCH_PUSH_JOB_FAILED_NON_USER_ERROR, 4);
  }

  @Test
  public void testRecordDataWriterSinkWriteTimeDistinguishesSinks() {
    stats.recordDataWriterSinkWriteTime(
        TEST_STORE_NAME,
        PushType.BATCH,
        VenicePushJobDataWriterSink.EXTERNAL_STORAGE,
        1500);
    stats.recordDataWriterSinkWriteTime(TEST_STORE_NAME, PushType.BATCH, VenicePushJobDataWriterSink.VENICE, 400);

    validateSinkWriteTime(1500, 1, sinkAttributes(PushType.BATCH, VenicePushJobDataWriterSink.EXTERNAL_STORAGE));
    validateSinkWriteTime(400, 1, sinkAttributes(PushType.BATCH, VenicePushJobDataWriterSink.VENICE));
  }

  @Test
  public void testRecordDataWriterSinkWriteTimeForIncrementalPush() {
    stats.recordDataWriterSinkWriteTime(TEST_STORE_NAME, PushType.INCREMENTAL, VenicePushJobDataWriterSink.VENICE, 250);
    validateSinkWriteTime(250, 1, sinkAttributes(PushType.INCREMENTAL, VenicePushJobDataWriterSink.VENICE));
  }

  @Test
  public void testRecordDataWriterSinkWriteTimeSkipsNegativeValues() {
    stats.recordDataWriterSinkWriteTime(
        TEST_STORE_NAME,
        PushType.BATCH,
        VenicePushJobDataWriterSink.EXTERNAL_STORAGE,
        -1);
    stats.recordDataWriterSinkWriteTime(TEST_STORE_NAME, PushType.BATCH, VenicePushJobDataWriterSink.VENICE, -1);

    // A push that never reported a leg must leave the distribution untouched rather than contribute a zero:
    // with no observation at all the instrument never even materializes.
    assertTrue(
        inMemoryMetricReader.collectAllMetrics()
            .stream()
            .noneMatch(
                metricData -> metricData.getName()
                    .endsWith(
                        PushJobStatusStats.PushJobOtelMetricEntity.PUSH_JOB_DATA_WRITER_SINK_WRITE_TIME
                            .getMetricName())),
        "No observation should be recorded for a negative duration");
  }

  @Test
  public void testRecordDataWriterSinkWriteTimeAcceptsZero() {
    stats.recordDataWriterSinkWriteTime(TEST_STORE_NAME, PushType.BATCH, VenicePushJobDataWriterSink.VENICE, 0);
    ExponentialHistogramPointData pointData =
        getSinkWriteTimeHistogram(sinkAttributes(PushType.BATCH, VenicePushJobDataWriterSink.VENICE));
    assertNotNull(pointData, "Zero is a legitimate duration and must be recorded");
    assertEquals(pointData.getCount(), 1);
  }

  @Test
  public void testRecordExternalStorageWriteFailureCarriesRegion() {
    stats.recordExternalStorageWriteFailure(TEST_STORE_NAME, "dc-1");

    validateCounter(
        PushJobStatusStats.PushJobOtelMetricEntity.PUSH_JOB_EXTERNAL_STORAGE_WRITE_FAILURE_COUNT.getMetricName(),
        1,
        externalStorageWriteFailureAttributes(TEST_STORE_NAME, "dc-1"));
  }

  /**
   * Alerting is per fabric, so two regions failing for the same store must stay two separate time series rather
   * than collapsing into one counter.
   */
  @Test
  public void testRecordExternalStorageWriteFailureSeparatesRegions() {
    stats.recordExternalStorageWriteFailure(TEST_STORE_NAME, "dc-0");
    stats.recordExternalStorageWriteFailure(TEST_STORE_NAME, "dc-1");
    stats.recordExternalStorageWriteFailure(TEST_STORE_NAME, "dc-1");

    validateCounter(
        PushJobStatusStats.PushJobOtelMetricEntity.PUSH_JOB_EXTERNAL_STORAGE_WRITE_FAILURE_COUNT.getMetricName(),
        1,
        externalStorageWriteFailureAttributes(TEST_STORE_NAME, "dc-0"));
    validateCounter(
        PushJobStatusStats.PushJobOtelMetricEntity.PUSH_JOB_EXTERNAL_STORAGE_WRITE_FAILURE_COUNT.getMetricName(),
        2,
        externalStorageWriteFailureAttributes(TEST_STORE_NAME, "dc-1"));
  }

  @Test
  public void testExternalStorageWriteFailureNotRecordedWhenNothingFailed() {
    stats.recordBatchPushSuccessSensor(TEST_STORE_NAME, COMPLETED, UNDER_SLA);

    assertTrue(
        inMemoryMetricReader.collectAllMetrics()
            .stream()
            .noneMatch(
                metricData -> metricData.getName()
                    .endsWith(
                        PushJobStatusStats.PushJobOtelMetricEntity.PUSH_JOB_EXTERNAL_STORAGE_WRITE_FAILURE_COUNT
                            .getMetricName())),
        "A healthy push must not materialize the external storage write failure counter");
  }

  private static Attributes externalStorageWriteFailureAttributes(String storeName, String regionName) {
    return Attributes.builder()
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), storeName)
        .put(VENICE_REGION_NAME.getDimensionNameInDefaultFormat(), regionName)
        .build();
  }

  @Test
  public void testNoNpeWhenOtelDisabled() {
    VeniceMetricsRepository disabledRepo = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX)
            .setEmitOtelMetrics(false)
            .setTehutiMetricConfig(MetricsRepositoryUtils.createDefaultSingleThreadedMetricConfig())
            .build());
    PushJobStatusStats disabledStats = new PushJobStatusStats(disabledRepo, TEST_CLUSTER_NAME);

    disabledStats.recordBatchPushSuccessSensor(TEST_STORE_NAME, COMPLETED, UNDER_SLA);
    disabledStats.recordBatchPushFailureDueToUserErrorSensor(TEST_STORE_NAME, ERROR, AT_OR_OVER_SLA);
    disabledStats.recordBatchPushFailureNotDueToUserErrorSensor(TEST_STORE_NAME, KILLED, UNDER_SLA);
    disabledStats.recordIncrementalPushSuccessSensor(TEST_STORE_NAME, COMPLETED, AT_OR_OVER_SLA);
    disabledStats.recordIncrementalPushFailureDueToUserErrorSensor(TEST_STORE_NAME, KILLED, UNDER_SLA);
    disabledStats.recordIncrementalPushFailureNotDueToUserErrorSensor(TEST_STORE_NAME, ERROR, AT_OR_OVER_SLA);
    disabledStats.recordExternalStorageWriteFailure(TEST_STORE_NAME, "dc-0");

    for (PushJobTehutiMetricNameEnum metricName: new PushJobTehutiMetricNameEnum[] {
        PushJobTehutiMetricNameEnum.BATCH_PUSH_JOB_SUCCESS,
        PushJobTehutiMetricNameEnum.BATCH_PUSH_JOB_FAILED_USER_ERROR,
        PushJobTehutiMetricNameEnum.BATCH_PUSH_JOB_FAILED_NON_USER_ERROR,
        PushJobTehutiMetricNameEnum.INCREMENTAL_PUSH_JOB_SUCCESS,
        PushJobTehutiMetricNameEnum.INCREMENTAL_PUSH_JOB_FAILED_USER_ERROR,
        PushJobTehutiMetricNameEnum.INCREMENTAL_PUSH_JOB_FAILED_NON_USER_ERROR }) {
      validateTehutiCount(disabledRepo, metricName, 1);
    }
  }

  private static Attributes pushJobAttributes(
      String storeName,
      PushType pushType,
      VenicePushJobStatus status,
      PushJobDetailsStatus executionState,
      VenicePushJobDurationBucket durationBucket) {
    return Attributes.builder()
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), storeName)
        .put(VENICE_PUSH_JOB_TYPE.getDimensionNameInDefaultFormat(), pushType.getDimensionValue())
        .put(VENICE_PUSH_JOB_STATUS.getDimensionNameInDefaultFormat(), status.getDimensionValue())
        .put(VENICE_PUSH_JOB_DURATION_BUCKET.getDimensionNameInDefaultFormat(), durationBucket.getDimensionValue())
        .put(VENICE_PUSH_JOB_EXECUTION_STATE.getDimensionNameInDefaultFormat(), executionState.getDimensionValue())
        .build();
  }

  private static void validateTehutiCount(
      VeniceMetricsRepository repository,
      PushJobTehutiMetricNameEnum metricName,
      double expectedCount) {
    String fullName =
        AbstractVeniceStats.getSensorFullName("." + TEST_CLUSTER_NAME, metricName.getMetricName()) + ".Count";
    assertNotNull(repository.getMetric(fullName));
    assertEquals(repository.getMetric(fullName).value(), expectedCount);
  }

  private void validateCounter(String metricName, long expectedValue, Attributes expectedAttributes) {
    OpenTelemetryDataTestUtils.validateLongPointDataFromCounter(
        inMemoryMetricReader,
        expectedValue,
        expectedAttributes,
        metricName,
        TEST_METRIC_PREFIX);
  }

  private static Attributes sinkAttributes(PushType pushType, VenicePushJobDataWriterSink sink) {
    return Attributes.builder()
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), TEST_STORE_NAME)
        .put(VENICE_PUSH_JOB_TYPE.getDimensionNameInDefaultFormat(), pushType.getDimensionValue())
        .put(VENICE_PUSH_JOB_DATA_WRITER_SINK.getDimensionNameInDefaultFormat(), sink.getDimensionValue())
        .build();
  }

  private void validateSinkWriteTime(double expectedValue, long expectedCount, Attributes expectedAttributes) {
    OpenTelemetryDataTestUtils.validateExponentialHistogramPointData(
        inMemoryMetricReader,
        expectedValue,
        expectedValue,
        expectedCount,
        expectedValue * expectedCount,
        expectedAttributes,
        PushJobStatusStats.PushJobOtelMetricEntity.PUSH_JOB_DATA_WRITER_SINK_WRITE_TIME.getMetricName(),
        TEST_METRIC_PREFIX);
  }

  private ExponentialHistogramPointData getSinkWriteTimeHistogram(Attributes expectedAttributes) {
    return OpenTelemetryDataTestUtils.getExponentialHistogramPointData(
        inMemoryMetricReader.collectAllMetrics(),
        PushJobStatusStats.PushJobOtelMetricEntity.PUSH_JOB_DATA_WRITER_SINK_WRITE_TIME.getMetricName(),
        TEST_METRIC_PREFIX,
        expectedAttributes);
  }
}
