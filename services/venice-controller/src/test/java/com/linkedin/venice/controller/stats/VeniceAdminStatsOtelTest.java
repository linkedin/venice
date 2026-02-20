package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.controller.VeniceController.CONTROLLER_SERVICE_METRIC_ENTITIES;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_PUSH_JOB_TYPE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.meta.Version.PushType;
import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.utils.OpenTelemetryDataTestUtils;
import com.linkedin.venice.utils.Utils;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.metrics.MetricsRepository;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
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

  @Test
  public void testVeniceAdminTehutiMetricNameEnum() {
    Map<VeniceAdminStats.VeniceAdminTehutiMetricNameEnum, String> expectedNames = new HashMap<>();
    expectedNames.put(
        VeniceAdminStats.VeniceAdminTehutiMetricNameEnum.UNEXPECTED_TOPIC_ABSENCE_COUNT,
        "unexpected_topic_absence_during_incremental_push_count");
    expectedNames.put(
        VeniceAdminStats.VeniceAdminTehutiMetricNameEnum.BATCH_PUSH_STARTED_COUNT,
        "successfully_started_user_batch_push_parent_admin_count");
    expectedNames.put(
        VeniceAdminStats.VeniceAdminTehutiMetricNameEnum.INCREMENTAL_PUSH_STARTED_COUNT,
        "successful_started_user_incremental_push_parent_admin_count");
    expectedNames.put(
        VeniceAdminStats.VeniceAdminTehutiMetricNameEnum.SERIALIZATION_FAILURE_COUNT,
        "failed_serializing_admin_operation_message_count");

    assertEquals(
        VeniceAdminStats.VeniceAdminTehutiMetricNameEnum.values().length,
        expectedNames.size(),
        "New VeniceAdminTehutiMetricNameEnum values were added but not included in this test");

    for (VeniceAdminStats.VeniceAdminTehutiMetricNameEnum enumValue: VeniceAdminStats.VeniceAdminTehutiMetricNameEnum
        .values()) {
      String expectedName = expectedNames.get(enumValue);
      assertNotNull(expectedName, "No expected metric name for " + enumValue.name());
      assertEquals(enumValue.getMetricName(), expectedName, "Unexpected metric name for " + enumValue.name());
    }
  }

  @Test
  public void testVeniceAdminOtelMetricEntity() {
    Map<VeniceAdminStats.VeniceAdminOtelMetricEntity, MetricEntity> expectedMetrics = new HashMap<>();
    expectedMetrics.put(
        VeniceAdminStats.VeniceAdminOtelMetricEntity.ADMIN_TOPIC_UNEXPECTED_ABSENCE_COUNT,
        new MetricEntity(
            "admin.topic.unexpected_absence_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Topics unexpectedly missing or truncated during push",
            Utils.setOf(VeniceMetricsDimensions.VENICE_CLUSTER_NAME, VeniceMetricsDimensions.VENICE_PUSH_JOB_TYPE)));
    expectedMetrics.put(
        VeniceAdminStats.VeniceAdminOtelMetricEntity.ADMIN_PUSH_STARTED_COUNT,
        new MetricEntity(
            "admin.push.started_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Successful push starts from parent admin, differentiated by push type",
            Utils.setOf(VeniceMetricsDimensions.VENICE_CLUSTER_NAME, VeniceMetricsDimensions.VENICE_PUSH_JOB_TYPE)));
    expectedMetrics.put(
        VeniceAdminStats.VeniceAdminOtelMetricEntity.ADMIN_OPERATION_SERIALIZATION_FAILURE_COUNT,
        new MetricEntity(
            "admin.operation.serialization.failure_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Failed admin operation serializations",
            Utils.setOf(VeniceMetricsDimensions.VENICE_CLUSTER_NAME)));

    assertEquals(
        VeniceAdminStats.VeniceAdminOtelMetricEntity.values().length,
        expectedMetrics.size(),
        "New VeniceAdminOtelMetricEntity values were added but not included in this test");

    for (VeniceAdminStats.VeniceAdminOtelMetricEntity metric: VeniceAdminStats.VeniceAdminOtelMetricEntity.values()) {
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

    // Verify all VeniceAdminOtelMetricEntity entries are present in CONTROLLER_SERVICE_METRIC_ENTITIES
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
