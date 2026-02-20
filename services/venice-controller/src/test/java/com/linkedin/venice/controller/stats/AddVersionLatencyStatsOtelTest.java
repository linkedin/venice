package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.controller.VeniceController.CONTROLLER_SERVICE_METRIC_ENTITIES;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_ADMIN_MESSAGE_PROCESSING_COMPONENT;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_ADMIN_MESSAGE_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.venice.controller.kafka.protocol.enums.AdminMessageType;
import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.dimensions.AdminMessageProcessingComponent;
import com.linkedin.venice.utils.OpenTelemetryDataTestUtils;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.metrics.MetricsRepository;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class AddVersionLatencyStatsOtelTest {
  private static final String TEST_METRIC_PREFIX = "controller";
  private static final String TEST_CLUSTER_NAME = "test-cluster";
  private static final String RESOURCE_NAME = ".test-cluster";
  private InMemoryMetricReader inMemoryMetricReader;
  private VeniceMetricsRepository metricsRepository;
  private AddVersionLatencyStats stats;

  @BeforeMethod
  public void setUp() {
    this.inMemoryMetricReader = InMemoryMetricReader.create();
    metricsRepository = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX)
            .setMetricEntities(CONTROLLER_SERVICE_METRIC_ENTITIES)
            .setEmitOtelMetrics(true)
            .setOtelAdditionalMetricsReader(inMemoryMetricReader)
            .build());

    stats = new AddVersionLatencyStats(metricsRepository, TEST_CLUSTER_NAME);
  }

  @Test
  public void testRecordRetireOldVersionsLatency() {
    stats.recordRetireOldVersionsLatency(100);

    validateHistogram(
        AddVersionLatencyStats.AddVersionLatencyOtelMetricEntity.ADMIN_CONSUMPTION_MESSAGE_PHASE_START_TO_END_PROCESSING_TIME_PER_COMPONENT
            .getMetricName(),
        100.0,
        100.0,
        1,
        100.0,
        componentAttributes(AdminMessageProcessingComponent.RETIRE_OLD_VERSIONS));

    validateTehutiMetric(
        AddVersionLatencyStats.AddVersionLatencyTehutiMetricNameEnum.ADD_VERSION_RETIRE_OLD_VERSIONS_LATENCY,
        "Avg",
        100.0);
    validateTehutiMetric(
        AddVersionLatencyStats.AddVersionLatencyTehutiMetricNameEnum.ADD_VERSION_RETIRE_OLD_VERSIONS_LATENCY,
        "Max",
        100.0);
  }

  @Test
  public void testRecordResourceAssignmentWaitLatency() {
    stats.recordResourceAssignmentWaitLatency(200);

    validateHistogram(
        AddVersionLatencyStats.AddVersionLatencyOtelMetricEntity.ADMIN_CONSUMPTION_MESSAGE_PHASE_START_TO_END_PROCESSING_TIME_PER_COMPONENT
            .getMetricName(),
        200.0,
        200.0,
        1,
        200.0,
        componentAttributes(AdminMessageProcessingComponent.RESOURCE_ASSIGNMENT_WAIT));

    validateTehutiMetric(
        AddVersionLatencyStats.AddVersionLatencyTehutiMetricNameEnum.ADD_VERSION_RESOURCE_ASSIGNMENT_WAIT_LATENCY,
        "Avg",
        200.0);
    validateTehutiMetric(
        AddVersionLatencyStats.AddVersionLatencyTehutiMetricNameEnum.ADD_VERSION_RESOURCE_ASSIGNMENT_WAIT_LATENCY,
        "Max",
        200.0);
  }

  @Test
  public void testRecordVersionCreationFailureLatency() {
    stats.recordVersionCreationFailureLatency(300);

    validateHistogram(
        AddVersionLatencyStats.AddVersionLatencyOtelMetricEntity.ADMIN_CONSUMPTION_MESSAGE_PHASE_START_TO_END_PROCESSING_TIME_PER_COMPONENT
            .getMetricName(),
        300.0,
        300.0,
        1,
        300.0,
        componentAttributes(AdminMessageProcessingComponent.FAILURE_HANDLING));

    validateTehutiMetric(
        AddVersionLatencyStats.AddVersionLatencyTehutiMetricNameEnum.ADD_VERSION_CREATION_FAILURE_LATENCY,
        "Avg",
        300.0);
    validateTehutiMetric(
        AddVersionLatencyStats.AddVersionLatencyTehutiMetricNameEnum.ADD_VERSION_CREATION_FAILURE_LATENCY,
        "Max",
        300.0);
  }

  @Test
  public void testRecordExistingSourceVersionHandlingLatency() {
    stats.recordExistingSourceVersionHandlingLatency(400);

    validateHistogram(
        AddVersionLatencyStats.AddVersionLatencyOtelMetricEntity.ADMIN_CONSUMPTION_MESSAGE_PHASE_START_TO_END_PROCESSING_TIME_PER_COMPONENT
            .getMetricName(),
        400.0,
        400.0,
        1,
        400.0,
        componentAttributes(AdminMessageProcessingComponent.EXISTING_VERSION_HANDLING));

    validateTehutiMetric(
        AddVersionLatencyStats.AddVersionLatencyTehutiMetricNameEnum.ADD_VERSION_EXISTING_SOURCE_HANDLING_LATENCY,
        "Avg",
        400.0);
    validateTehutiMetric(
        AddVersionLatencyStats.AddVersionLatencyTehutiMetricNameEnum.ADD_VERSION_EXISTING_SOURCE_HANDLING_LATENCY,
        "Max",
        400.0);
  }

  @Test
  public void testRecordStartOfPushLatency() {
    stats.recordStartOfPushLatency(500);

    validateHistogram(
        AddVersionLatencyStats.AddVersionLatencyOtelMetricEntity.ADMIN_CONSUMPTION_MESSAGE_PHASE_START_TO_END_PROCESSING_TIME_PER_COMPONENT
            .getMetricName(),
        500.0,
        500.0,
        1,
        500.0,
        componentAttributes(AdminMessageProcessingComponent.START_OF_PUSH));

    validateTehutiMetric(
        AddVersionLatencyStats.AddVersionLatencyTehutiMetricNameEnum.ADD_VERSION_START_OF_PUSH_LATENCY,
        "Avg",
        500.0);
    validateTehutiMetric(
        AddVersionLatencyStats.AddVersionLatencyTehutiMetricNameEnum.ADD_VERSION_START_OF_PUSH_LATENCY,
        "Max",
        500.0);
  }

  @Test
  public void testRecordBatchTopicCreationLatency() {
    stats.recordBatchTopicCreationLatency(600);

    validateHistogram(
        AddVersionLatencyStats.AddVersionLatencyOtelMetricEntity.ADMIN_CONSUMPTION_MESSAGE_PHASE_START_TO_END_PROCESSING_TIME_PER_COMPONENT
            .getMetricName(),
        600.0,
        600.0,
        1,
        600.0,
        componentAttributes(AdminMessageProcessingComponent.BATCH_TOPIC_CREATION));

    validateTehutiMetric(
        AddVersionLatencyStats.AddVersionLatencyTehutiMetricNameEnum.ADD_VERSION_BATCH_TOPIC_CREATION_LATENCY,
        "Avg",
        600.0);
    validateTehutiMetric(
        AddVersionLatencyStats.AddVersionLatencyTehutiMetricNameEnum.ADD_VERSION_BATCH_TOPIC_CREATION_LATENCY,
        "Max",
        600.0);
  }

  @Test
  public void testRecordHelixResourceCreationLatency() {
    stats.recordHelixResourceCreationLatency(700);

    validateHistogram(
        AddVersionLatencyStats.AddVersionLatencyOtelMetricEntity.ADMIN_CONSUMPTION_MESSAGE_PHASE_START_TO_END_PROCESSING_TIME_PER_COMPONENT
            .getMetricName(),
        700.0,
        700.0,
        1,
        700.0,
        componentAttributes(AdminMessageProcessingComponent.HELIX_RESOURCE_CREATION));

    validateTehutiMetric(
        AddVersionLatencyStats.AddVersionLatencyTehutiMetricNameEnum.ADD_VERSION_HELIX_RESOURCE_CREATION_LATENCY,
        "Avg",
        700.0);
    validateTehutiMetric(
        AddVersionLatencyStats.AddVersionLatencyTehutiMetricNameEnum.ADD_VERSION_HELIX_RESOURCE_CREATION_LATENCY,
        "Max",
        700.0);
  }

  @Test
  public void testNoNpeWhenOtelDisabled() {
    VeniceMetricsRepository disabledRepo = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX).setEmitOtelMetrics(false).build());
    AddVersionLatencyStats disabledStats = new AddVersionLatencyStats(disabledRepo, TEST_CLUSTER_NAME);

    disabledStats.recordRetireOldVersionsLatency(10);
    disabledStats.recordResourceAssignmentWaitLatency(10);
    disabledStats.recordVersionCreationFailureLatency(10);
    disabledStats.recordExistingSourceVersionHandlingLatency(10);
    disabledStats.recordStartOfPushLatency(10);
    disabledStats.recordBatchTopicCreationLatency(10);
    disabledStats.recordHelixResourceCreationLatency(10);
  }

  @Test
  public void testNoNpeWhenPlainMetricsRepository() {
    MetricsRepository plainRepo = new MetricsRepository();
    AddVersionLatencyStats plainStats = new AddVersionLatencyStats(plainRepo, TEST_CLUSTER_NAME);

    plainStats.recordRetireOldVersionsLatency(10);
    plainStats.recordResourceAssignmentWaitLatency(10);
    plainStats.recordVersionCreationFailureLatency(10);
    plainStats.recordExistingSourceVersionHandlingLatency(10);
    plainStats.recordStartOfPushLatency(10);
    plainStats.recordBatchTopicCreationLatency(10);
    plainStats.recordHelixResourceCreationLatency(10);
  }

  @Test
  public void testAddVersionLatencyTehutiMetricNameEnum() {
    Map<AddVersionLatencyStats.AddVersionLatencyTehutiMetricNameEnum, String> expectedNames = new HashMap<>();
    expectedNames.put(
        AddVersionLatencyStats.AddVersionLatencyTehutiMetricNameEnum.ADD_VERSION_RETIRE_OLD_VERSIONS_LATENCY,
        "add_version_retire_old_versions_latency");
    expectedNames.put(
        AddVersionLatencyStats.AddVersionLatencyTehutiMetricNameEnum.ADD_VERSION_RESOURCE_ASSIGNMENT_WAIT_LATENCY,
        "add_version_resource_assignment_wait_latency");
    expectedNames.put(
        AddVersionLatencyStats.AddVersionLatencyTehutiMetricNameEnum.ADD_VERSION_CREATION_FAILURE_LATENCY,
        "add_version_creation_failure_latency");
    expectedNames.put(
        AddVersionLatencyStats.AddVersionLatencyTehutiMetricNameEnum.ADD_VERSION_EXISTING_SOURCE_HANDLING_LATENCY,
        "add_version_existing_source_handling_latency");
    expectedNames.put(
        AddVersionLatencyStats.AddVersionLatencyTehutiMetricNameEnum.ADD_VERSION_START_OF_PUSH_LATENCY,
        "add_version_start_of_push_latency");
    expectedNames.put(
        AddVersionLatencyStats.AddVersionLatencyTehutiMetricNameEnum.ADD_VERSION_BATCH_TOPIC_CREATION_LATENCY,
        "add_version_batch_topic_creation_latency");
    expectedNames.put(
        AddVersionLatencyStats.AddVersionLatencyTehutiMetricNameEnum.ADD_VERSION_HELIX_RESOURCE_CREATION_LATENCY,
        "add_version_helix_resource_creation_latency");

    assertEquals(
        AddVersionLatencyStats.AddVersionLatencyTehutiMetricNameEnum.values().length,
        expectedNames.size(),
        "New AddVersionLatencyTehutiMetricNameEnum values were added but not included in this test");

    for (AddVersionLatencyStats.AddVersionLatencyTehutiMetricNameEnum enumValue: AddVersionLatencyStats.AddVersionLatencyTehutiMetricNameEnum
        .values()) {
      String expectedName = expectedNames.get(enumValue);
      assertNotNull(expectedName, "No expected metric name for " + enumValue.name());
      assertEquals(enumValue.getMetricName(), expectedName, "Unexpected metric name for " + enumValue.name());
    }
  }

  private Attributes componentAttributes(AdminMessageProcessingComponent component) {
    return Attributes.builder()
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
        .put(
            VENICE_ADMIN_MESSAGE_TYPE.getDimensionNameInDefaultFormat(),
            AdminMessageType.ADD_VERSION.getDimensionValue())
        .put(VENICE_ADMIN_MESSAGE_PROCESSING_COMPONENT.getDimensionNameInDefaultFormat(), component.getDimensionValue())
        .build();
  }

  private void validateHistogram(
      String metricName,
      double expectedMin,
      double expectedMax,
      long expectedCount,
      double expectedSum,
      Attributes expectedAttributes) {
    OpenTelemetryDataTestUtils.validateExponentialHistogramPointData(
        inMemoryMetricReader,
        expectedMin,
        expectedMax,
        expectedCount,
        expectedSum,
        expectedAttributes,
        metricName,
        TEST_METRIC_PREFIX);
  }

  private void validateTehutiMetric(
      AddVersionLatencyStats.AddVersionLatencyTehutiMetricNameEnum tehutiEnum,
      String statSuffix,
      double expectedValue) {
    String tehutiMetricName =
        AbstractVeniceStats.getSensorFullName(RESOURCE_NAME, tehutiEnum.getMetricName()) + "." + statSuffix;
    assertNotNull(metricsRepository.getMetric(tehutiMetricName), "Tehuti metric should exist: " + tehutiMetricName);
    assertEquals(
        metricsRepository.getMetric(tehutiMetricName).value(),
        expectedValue,
        "Tehuti metric value mismatch for: " + tehutiMetricName);
  }
}
