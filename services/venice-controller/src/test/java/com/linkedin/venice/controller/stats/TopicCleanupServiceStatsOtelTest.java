package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.controller.VeniceController.CONTROLLER_SERVICE_METRIC_ENTITIES;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY;

import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory;
import com.linkedin.venice.utils.OpenTelemetryDataTestUtils;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.metrics.MetricsRepository;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TopicCleanupServiceStatsOtelTest {
  private static final String TEST_METRIC_PREFIX = "controller";
  private InMemoryMetricReader inMemoryMetricReader;
  private TopicCleanupServiceStats stats;

  @BeforeMethod
  public void setUp() {
    this.inMemoryMetricReader = InMemoryMetricReader.create();
    VeniceMetricsRepository metricsRepository = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX)
            .setMetricEntities(CONTROLLER_SERVICE_METRIC_ENTITIES)
            .setEmitOtelMetrics(true)
            .setOtelAdditionalMetricsReader(inMemoryMetricReader)
            .build());

    stats = new TopicCleanupServiceStats(metricsRepository);
  }

  @Test
  public void testRecordDeletableTopicsCount() {
    stats.recordDeletableTopicsCount(5);
    validateGauge(
        TopicCleanupServiceStats.TopicCleanupOtelMetricEntity.TOPIC_CLEANUP_DELETABLE_COUNT.getMetricName(),
        5,
        Attributes.empty());
  }

  @Test
  public void testRecordDeletableTopicsCountUpdatesGauge() {
    stats.recordDeletableTopicsCount(10);
    stats.recordDeletableTopicsCount(3);
    // Gauge should reflect the last recorded value
    validateGauge(
        TopicCleanupServiceStats.TopicCleanupOtelMetricEntity.TOPIC_CLEANUP_DELETABLE_COUNT.getMetricName(),
        3,
        Attributes.empty());
  }

  @Test
  public void testRecordTopicDeleted() {
    stats.recordTopicDeleted();
    validateCounter(
        TopicCleanupServiceStats.TopicCleanupOtelMetricEntity.TOPIC_CLEANUP_DELETED_COUNT.getMetricName(),
        1,
        Attributes.builder()
            .put(
                VENICE_RESPONSE_STATUS_CODE_CATEGORY.getDimensionNameInDefaultFormat(),
                VeniceResponseStatusCategory.SUCCESS.getDimensionValue())
            .build());
  }

  @Test
  public void testRecordMultipleTopicDeletions() {
    stats.recordTopicDeleted();
    stats.recordTopicDeleted();
    stats.recordTopicDeleted();
    validateCounter(
        TopicCleanupServiceStats.TopicCleanupOtelMetricEntity.TOPIC_CLEANUP_DELETED_COUNT.getMetricName(),
        3,
        Attributes.builder()
            .put(
                VENICE_RESPONSE_STATUS_CODE_CATEGORY.getDimensionNameInDefaultFormat(),
                VeniceResponseStatusCategory.SUCCESS.getDimensionValue())
            .build());
  }

  @Test
  public void testRecordTopicDeletionError() {
    stats.recordTopicDeletionError();
    validateCounter(
        TopicCleanupServiceStats.TopicCleanupOtelMetricEntity.TOPIC_CLEANUP_DELETED_COUNT.getMetricName(),
        1,
        Attributes.builder()
            .put(
                VENICE_RESPONSE_STATUS_CODE_CATEGORY.getDimensionNameInDefaultFormat(),
                VeniceResponseStatusCategory.FAIL.getDimensionValue())
            .build());
  }

  @Test
  public void testRecordMultipleTopicDeletionErrors() {
    stats.recordTopicDeletionError();
    stats.recordTopicDeletionError();
    validateCounter(
        TopicCleanupServiceStats.TopicCleanupOtelMetricEntity.TOPIC_CLEANUP_DELETED_COUNT.getMetricName(),
        2,
        Attributes.builder()
            .put(
                VENICE_RESPONSE_STATUS_CODE_CATEGORY.getDimensionNameInDefaultFormat(),
                VeniceResponseStatusCategory.FAIL.getDimensionValue())
            .build());
  }

  @Test
  public void testSuccessAndFailureCountersAreIndependent() {
    stats.recordTopicDeleted();
    stats.recordTopicDeleted();
    stats.recordTopicDeletionError();

    // Verify success counter
    validateCounter(
        TopicCleanupServiceStats.TopicCleanupOtelMetricEntity.TOPIC_CLEANUP_DELETED_COUNT.getMetricName(),
        2,
        Attributes.builder()
            .put(
                VENICE_RESPONSE_STATUS_CODE_CATEGORY.getDimensionNameInDefaultFormat(),
                VeniceResponseStatusCategory.SUCCESS.getDimensionValue())
            .build());

    // Verify failure counter is independent
    validateCounter(
        TopicCleanupServiceStats.TopicCleanupOtelMetricEntity.TOPIC_CLEANUP_DELETED_COUNT.getMetricName(),
        1,
        Attributes.builder()
            .put(
                VENICE_RESPONSE_STATUS_CODE_CATEGORY.getDimensionNameInDefaultFormat(),
                VeniceResponseStatusCategory.FAIL.getDimensionValue())
            .build());
  }

  @Test
  public void testAllMetricsRecordedTogether() {
    stats.recordDeletableTopicsCount(15);
    stats.recordTopicDeleted();
    stats.recordTopicDeletionError();

    // Gauge metric
    validateGauge(
        TopicCleanupServiceStats.TopicCleanupOtelMetricEntity.TOPIC_CLEANUP_DELETABLE_COUNT.getMetricName(),
        15,
        Attributes.empty());

    // Success counter
    validateCounter(
        TopicCleanupServiceStats.TopicCleanupOtelMetricEntity.TOPIC_CLEANUP_DELETED_COUNT.getMetricName(),
        1,
        Attributes.builder()
            .put(
                VENICE_RESPONSE_STATUS_CODE_CATEGORY.getDimensionNameInDefaultFormat(),
                VeniceResponseStatusCategory.SUCCESS.getDimensionValue())
            .build());

    // Failure counter
    validateCounter(
        TopicCleanupServiceStats.TopicCleanupOtelMetricEntity.TOPIC_CLEANUP_DELETED_COUNT.getMetricName(),
        1,
        Attributes.builder()
            .put(
                VENICE_RESPONSE_STATUS_CODE_CATEGORY.getDimensionNameInDefaultFormat(),
                VeniceResponseStatusCategory.FAIL.getDimensionValue())
            .build());
  }

  @Test
  public void testNoNpeWhenOtelDisabled() {
    VeniceMetricsRepository disabledRepo = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX).setEmitOtelMetrics(false).build());
    TopicCleanupServiceStats disabledStats = new TopicCleanupServiceStats(disabledRepo);

    // All methods should execute without NPE
    disabledStats.recordDeletableTopicsCount(5);
    disabledStats.recordTopicDeleted();
    disabledStats.recordTopicDeletionError();
  }

  @Test
  public void testNoNpeWhenPlainMetricsRepository() {
    // When using a plain Tehuti MetricsRepository (not VeniceMetricsRepository),
    // OTel should be disabled and methods should not throw
    MetricsRepository plainRepo = new MetricsRepository();
    TopicCleanupServiceStats plainStats = new TopicCleanupServiceStats(plainRepo);

    plainStats.recordDeletableTopicsCount(5);
    plainStats.recordTopicDeleted();
    plainStats.recordTopicDeletionError();
  }

  private void validateCounter(String metricName, long expectedValue, Attributes expectedAttributes) {
    OpenTelemetryDataTestUtils.validateLongPointDataFromCounter(
        inMemoryMetricReader,
        expectedValue,
        expectedAttributes,
        metricName,
        TEST_METRIC_PREFIX);
  }

  private void validateGauge(String metricName, long expectedValue, Attributes expectedAttributes) {
    OpenTelemetryDataTestUtils.validateLongPointDataFromGauge(
        inMemoryMetricReader,
        expectedValue,
        expectedAttributes,
        metricName,
        TEST_METRIC_PREFIX);
  }
}
