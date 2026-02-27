package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.controller.VeniceController.CONTROLLER_SERVICE_METRIC_ENTITIES;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory;
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

  @Test
  public void testTopicCleanupOtelMetricEntity() {
    Map<TopicCleanupServiceStats.TopicCleanupOtelMetricEntity, MetricEntity> expectedMetrics = new HashMap<>();
    expectedMetrics.put(
        TopicCleanupServiceStats.TopicCleanupOtelMetricEntity.TOPIC_CLEANUP_DELETABLE_COUNT,
        MetricEntity.createWithNoDimensions(
            "topic_cleanup_service.topic.deletable_count",
            MetricType.GAUGE,
            MetricUnit.NUMBER,
            "Count of topics currently eligible for deletion"));
    expectedMetrics.put(
        TopicCleanupServiceStats.TopicCleanupOtelMetricEntity.TOPIC_CLEANUP_DELETED_COUNT,
        new MetricEntity(
            "topic_cleanup_service.topic.deleted_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of topic deletion operations",
            Utils.setOf(VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY)));

    assertEquals(
        TopicCleanupServiceStats.TopicCleanupOtelMetricEntity.values().length,
        expectedMetrics.size(),
        "New TopicCleanupOtelMetricEntity values were added but not included in this test");

    for (TopicCleanupServiceStats.TopicCleanupOtelMetricEntity metric: TopicCleanupServiceStats.TopicCleanupOtelMetricEntity
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

    // Verify all TopicCleanupOtelMetricEntity entries are present in CONTROLLER_SERVICE_METRIC_ENTITIES
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

  @Test
  public void testTopicCleanupTehutiMetricNameEnum() {
    Map<TopicCleanupServiceStats.TopicCleanupTehutiMetricNameEnum, String> expectedNames = new HashMap<>();
    expectedNames.put(
        TopicCleanupServiceStats.TopicCleanupTehutiMetricNameEnum.DELETABLE_TOPICS_COUNT,
        "deletable_topics_count");
    expectedNames
        .put(TopicCleanupServiceStats.TopicCleanupTehutiMetricNameEnum.TOPICS_DELETED_RATE, "topics_deleted_rate");
    expectedNames.put(
        TopicCleanupServiceStats.TopicCleanupTehutiMetricNameEnum.TOPIC_DELETION_ERROR_RATE,
        "topic_deletion_error_rate");

    assertEquals(
        TopicCleanupServiceStats.TopicCleanupTehutiMetricNameEnum.values().length,
        expectedNames.size(),
        "New TopicCleanupTehutiMetricNameEnum values were added but not included in this test");

    for (TopicCleanupServiceStats.TopicCleanupTehutiMetricNameEnum enumValue: TopicCleanupServiceStats.TopicCleanupTehutiMetricNameEnum
        .values()) {
      String expectedName = expectedNames.get(enumValue);
      assertNotNull(expectedName, "No expected metric name for " + enumValue.name());
      assertEquals(enumValue.getMetricName(), expectedName, "Unexpected metric name for " + enumValue.name());
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

  private void validateGauge(String metricName, long expectedValue, Attributes expectedAttributes) {
    OpenTelemetryDataTestUtils.validateLongPointDataFromGauge(
        inMemoryMetricReader,
        expectedValue,
        expectedAttributes,
        metricName,
        TEST_METRIC_PREFIX);
  }
}
