package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.controller.VeniceController.CONTROLLER_SERVICE_METRIC_ENTITIES;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_PUSH_JOB_DATA_WRITER_SINK;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_PUSH_JOB_STATUS;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_PUSH_JOB_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REGION_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.meta.Version.PushType;
import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.dimensions.VenicePushJobDataWriterSink;
import com.linkedin.venice.stats.dimensions.VenicePushJobStatus;
import com.linkedin.venice.utils.OpenTelemetryDataTestUtils;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.metrics.data.ExponentialHistogramPointData;
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
    stats.recordBatchPushSuccessSensor(TEST_STORE_NAME);

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
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX).setEmitOtelMetrics(false).build());
    PushJobStatusStats disabledStats = new PushJobStatusStats(disabledRepo, TEST_CLUSTER_NAME);

    disabledStats.recordBatchPushSuccessSensor(TEST_STORE_NAME);
    disabledStats.recordBatchPushFailureDueToUserErrorSensor(TEST_STORE_NAME);
    disabledStats.recordBatchPushFailureNotDueToUserErrorSensor(TEST_STORE_NAME);
    disabledStats.recordIncrementalPushSuccessSensor(TEST_STORE_NAME);
    disabledStats.recordIncrementalPushFailureDueToUserErrorSensor(TEST_STORE_NAME);
    disabledStats.recordIncrementalPushFailureNotDueToUserErrorSensor(TEST_STORE_NAME);
    disabledStats.recordExternalStorageWriteFailure(TEST_STORE_NAME, "dc-0");
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
