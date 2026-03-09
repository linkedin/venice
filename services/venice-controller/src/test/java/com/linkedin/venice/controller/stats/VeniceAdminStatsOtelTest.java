package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.controller.VeniceController.CONTROLLER_SERVICE_METRIC_ENTITIES;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_PUSH_JOB_TYPE;

import com.linkedin.venice.meta.Version.PushType;
import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.utils.OpenTelemetryDataTestUtils;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.metrics.MetricsRepository;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class VeniceAdminStatsOtelTest {
  private static final String TEST_METRIC_PREFIX = "controller";
  private static final String TEST_CLUSTER_NAME = "test-cluster";
  private InMemoryMetricReader inMemoryMetricReader;
  private VeniceAdminStats stats;

  @BeforeMethod
  public void setUp() {
    this.inMemoryMetricReader = InMemoryMetricReader.create();
    VeniceMetricsRepository metricsRepository = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX)
            .setMetricEntities(CONTROLLER_SERVICE_METRIC_ENTITIES)
            .setEmitOtelMetrics(true)
            .setOtelAdditionalMetricsReader(inMemoryMetricReader)
            .build());

    stats = new VeniceAdminStats(metricsRepository, "venice-admin-", TEST_CLUSTER_NAME);
  }

  @Test
  public void testRecordUnexpectedTopicAbsenceIncremental() {
    stats.recordUnexpectedTopicAbsenceCount(PushType.INCREMENTAL);
    validateCounter(
        VeniceAdminStats.VeniceAdminOtelMetricEntity.ADMIN_TOPIC_UNEXPECTED_ABSENCE_COUNT.getMetricName(),
        1,
        Attributes.builder()
            .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
            .put(VENICE_PUSH_JOB_TYPE.getDimensionNameInDefaultFormat(), PushType.INCREMENTAL.getDimensionValue())
            .build());
  }

  @Test
  public void testRecordUnexpectedTopicAbsenceStream() {
    stats.recordUnexpectedTopicAbsenceCount(PushType.STREAM);
    validateCounter(
        VeniceAdminStats.VeniceAdminOtelMetricEntity.ADMIN_TOPIC_UNEXPECTED_ABSENCE_COUNT.getMetricName(),
        1,
        Attributes.builder()
            .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
            .put(VENICE_PUSH_JOB_TYPE.getDimensionNameInDefaultFormat(), PushType.STREAM.getDimensionValue())
            .build());
  }

  @Test
  public void testRecordBatchPushStarted() {
    stats.recordSuccessfullyStartedUserBatchPushParentAdminCount();
    validateCounter(
        VeniceAdminStats.VeniceAdminOtelMetricEntity.ADMIN_PUSH_STARTED_COUNT.getMetricName(),
        1,
        Attributes.builder()
            .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
            .put(VENICE_PUSH_JOB_TYPE.getDimensionNameInDefaultFormat(), PushType.BATCH.getDimensionValue())
            .build());
  }

  @Test
  public void testRecordIncrementalPushStarted() {
    stats.recordSuccessfullyStartedUserIncrementalPushParentAdminCount();
    validateCounter(
        VeniceAdminStats.VeniceAdminOtelMetricEntity.ADMIN_PUSH_STARTED_COUNT.getMetricName(),
        1,
        Attributes.builder()
            .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
            .put(VENICE_PUSH_JOB_TYPE.getDimensionNameInDefaultFormat(), PushType.INCREMENTAL.getDimensionValue())
            .build());
  }

  @Test
  public void testRecordMultiplePushStarts() {
    stats.recordSuccessfullyStartedUserBatchPushParentAdminCount();
    stats.recordSuccessfullyStartedUserBatchPushParentAdminCount();
    stats.recordSuccessfullyStartedUserIncrementalPushParentAdminCount();

    // Batch counter
    validateCounter(
        VeniceAdminStats.VeniceAdminOtelMetricEntity.ADMIN_PUSH_STARTED_COUNT.getMetricName(),
        2,
        Attributes.builder()
            .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
            .put(VENICE_PUSH_JOB_TYPE.getDimensionNameInDefaultFormat(), PushType.BATCH.getDimensionValue())
            .build());

    // Incremental counter is independent
    validateCounter(
        VeniceAdminStats.VeniceAdminOtelMetricEntity.ADMIN_PUSH_STARTED_COUNT.getMetricName(),
        1,
        Attributes.builder()
            .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
            .put(VENICE_PUSH_JOB_TYPE.getDimensionNameInDefaultFormat(), PushType.INCREMENTAL.getDimensionValue())
            .build());
  }

  @Test
  public void testRecordSerializationFailure() {
    stats.recordFailedSerializingAdminOperationMessageCount();
    validateCounter(
        VeniceAdminStats.VeniceAdminOtelMetricEntity.ADMIN_OPERATION_SERIALIZATION_FAILURE_COUNT.getMetricName(),
        1,
        Attributes.builder().put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME).build());
  }

  @Test
  public void testAllMetricsRecordedTogether() {
    stats.recordUnexpectedTopicAbsenceCount(PushType.BATCH);
    stats.recordSuccessfullyStartedUserBatchPushParentAdminCount();
    stats.recordSuccessfullyStartedUserIncrementalPushParentAdminCount();
    stats.recordFailedSerializingAdminOperationMessageCount();

    // Unexpected absence counter
    validateCounter(
        VeniceAdminStats.VeniceAdminOtelMetricEntity.ADMIN_TOPIC_UNEXPECTED_ABSENCE_COUNT.getMetricName(),
        1,
        Attributes.builder()
            .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
            .put(VENICE_PUSH_JOB_TYPE.getDimensionNameInDefaultFormat(), PushType.BATCH.getDimensionValue())
            .build());

    // Batch push started counter
    validateCounter(
        VeniceAdminStats.VeniceAdminOtelMetricEntity.ADMIN_PUSH_STARTED_COUNT.getMetricName(),
        1,
        Attributes.builder()
            .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
            .put(VENICE_PUSH_JOB_TYPE.getDimensionNameInDefaultFormat(), PushType.BATCH.getDimensionValue())
            .build());

    // Incremental push started counter
    validateCounter(
        VeniceAdminStats.VeniceAdminOtelMetricEntity.ADMIN_PUSH_STARTED_COUNT.getMetricName(),
        1,
        Attributes.builder()
            .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
            .put(VENICE_PUSH_JOB_TYPE.getDimensionNameInDefaultFormat(), PushType.INCREMENTAL.getDimensionValue())
            .build());

    // Serialization failure counter
    validateCounter(
        VeniceAdminStats.VeniceAdminOtelMetricEntity.ADMIN_OPERATION_SERIALIZATION_FAILURE_COUNT.getMetricName(),
        1,
        Attributes.builder().put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME).build());
  }

  @Test
  public void testNoNpeWhenOtelDisabled() {
    VeniceMetricsRepository disabledRepo = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX).setEmitOtelMetrics(false).build());
    VeniceAdminStats disabledStats = new VeniceAdminStats(disabledRepo, "venice-admin-", TEST_CLUSTER_NAME);

    // All methods should execute without NPE
    disabledStats.recordUnexpectedTopicAbsenceCount(PushType.INCREMENTAL);
    disabledStats.recordSuccessfullyStartedUserBatchPushParentAdminCount();
    disabledStats.recordSuccessfullyStartedUserIncrementalPushParentAdminCount();
    disabledStats.recordFailedSerializingAdminOperationMessageCount();
  }

  @Test
  public void testNoNpeWhenPlainMetricsRepository() {
    // When using a plain Tehuti MetricsRepository (not VeniceMetricsRepository),
    // OTel should be disabled and methods should not throw
    MetricsRepository plainRepo = new MetricsRepository();
    VeniceAdminStats plainStats = new VeniceAdminStats(plainRepo, "venice-admin-", TEST_CLUSTER_NAME);

    plainStats.recordUnexpectedTopicAbsenceCount(PushType.BATCH);
    plainStats.recordSuccessfullyStartedUserBatchPushParentAdminCount();
    plainStats.recordSuccessfullyStartedUserIncrementalPushParentAdminCount();
    plainStats.recordFailedSerializingAdminOperationMessageCount();
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
