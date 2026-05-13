package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.controller.VeniceController.CONTROLLER_SERVICE_METRIC_ENTITIES;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_ADMIN_MESSAGE_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.venice.controller.kafka.protocol.enums.AdminMessageType;
import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.utils.OpenTelemetryDataTestUtils;
import com.linkedin.venice.utils.metrics.MetricsRepositoryUtils;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.metrics.MetricsRepository;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class AdminConsumptionStatsOtelTest {
  private static final String TEST_METRIC_PREFIX = "controller";
  private static final String TEST_CLUSTER_NAME = "test-cluster";
  private static final String RESOURCE_NAME = ".test-cluster-admin_consumption_task";
  private InMemoryMetricReader inMemoryMetricReader;
  private VeniceMetricsRepository metricsRepository;
  private AdminConsumptionStats stats;

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

    stats = new AdminConsumptionStats(metricsRepository, TEST_CLUSTER_NAME);
  }

  // Counter tests

  @Test
  public void testRecordFailedAdminConsumption() {
    stats.recordFailedAdminConsumption();

    // OTel
    validateCounter(
        AdminConsumptionStats.AdminConsumptionOtelMetricEntity.ADMIN_CONSUMPTION_MESSAGE_FAILURE_COUNT.getMetricName(),
        1,
        clusterAttributes());

    // Tehuti
    validateTehutiMetric(
        AdminConsumptionStats.AdminConsumptionTehutiMetricNameEnum.FAILED_ADMIN_MESSAGES,
        "Count",
        1.0);
  }

  @Test
  public void testRecordRetriableFailure() {
    stats.recordFailedRetriableAdminConsumption();

    validateCounter(
        AdminConsumptionStats.AdminConsumptionOtelMetricEntity.ADMIN_CONSUMPTION_MESSAGE_RETRIABLE_FAILURE_COUNT
            .getMetricName(),
        1,
        clusterAttributes());

    validateTehutiMetric(
        AdminConsumptionStats.AdminConsumptionTehutiMetricNameEnum.FAILED_RETRIABLE_ADMIN_MESSAGES,
        "Count",
        1.0);
  }

  @Test
  public void testRecordDivFailure() {
    stats.recordAdminTopicDIVErrorReportCount();

    validateCounter(
        AdminConsumptionStats.AdminConsumptionOtelMetricEntity.ADMIN_CONSUMPTION_MESSAGE_DIV_FAILURE_COUNT
            .getMetricName(),
        1,
        clusterAttributes());

    validateTehutiMetric(
        AdminConsumptionStats.AdminConsumptionTehutiMetricNameEnum.ADMIN_MESSAGE_DIV_ERROR_REPORT_COUNT,
        "Count",
        1.0);
  }

  @Test
  public void testRecordFutureSchemaCount() {
    stats.recordAdminMessagesWithFutureProtocolVersionCount();

    validateCounter(
        AdminConsumptionStats.AdminConsumptionOtelMetricEntity.ADMIN_CONSUMPTION_MESSAGE_FUTURE_SCHEMA_COUNT
            .getMetricName(),
        1,
        clusterAttributes());

    validateTehutiMetric(
        AdminConsumptionStats.AdminConsumptionTehutiMetricNameEnum.ADMIN_MESSAGES_WITH_FUTURE_PROTOCOL_VERSION_COUNT,
        "Count",
        1.0);
  }

  // Histogram tests (phase metrics with AdminMessageType)

  @Test
  public void testRecordProduceToBrokerTime() {
    stats.recordAdminMessageMMLatency(100.0, AdminMessageType.UPDATE_STORE);

    validateHistogram(
        AdminConsumptionStats.AdminConsumptionOtelMetricEntity.ADMIN_CONSUMPTION_MESSAGE_PHASE_REPLICATION_TO_LOCAL_BROKER_TIME
            .getMetricName(),
        100.0,
        100.0,
        1,
        100.0,
        clusterAndTypeAttributes(AdminMessageType.UPDATE_STORE));

    validateTehutiMetric(
        AdminConsumptionStats.AdminConsumptionTehutiMetricNameEnum.ADMIN_MESSAGE_MM_LATENCY_MS,
        "Avg",
        100.0);
    validateTehutiMetric(
        AdminConsumptionStats.AdminConsumptionTehutiMetricNameEnum.ADMIN_MESSAGE_MM_LATENCY_MS,
        "Max",
        100.0);
  }

  @Test
  public void testRecordBrokerToQueueTime() {
    stats.recordAdminMessageDelegateLatency(200.0, AdminMessageType.UPDATE_STORE);

    validateHistogram(
        AdminConsumptionStats.AdminConsumptionOtelMetricEntity.ADMIN_CONSUMPTION_MESSAGE_PHASE_BROKER_TO_PROCESSING_QUEUE_TIME
            .getMetricName(),
        200.0,
        200.0,
        1,
        200.0,
        clusterAndTypeAttributes(AdminMessageType.UPDATE_STORE));

    validateTehutiMetric(
        AdminConsumptionStats.AdminConsumptionTehutiMetricNameEnum.ADMIN_MESSAGE_DELEGATE_LATENCY_MS,
        "Avg",
        200.0);
    validateTehutiMetric(
        AdminConsumptionStats.AdminConsumptionTehutiMetricNameEnum.ADMIN_MESSAGE_DELEGATE_LATENCY_MS,
        "Max",
        200.0);
  }

  @Test
  public void testRecordQueueToStartProcessingTime() {
    stats.recordAdminMessageStartProcessingLatency(50.0, AdminMessageType.UPDATE_STORE);

    validateHistogram(
        AdminConsumptionStats.AdminConsumptionOtelMetricEntity.ADMIN_CONSUMPTION_MESSAGE_PHASE_QUEUE_TO_START_PROCESSING_TIME
            .getMetricName(),
        50.0,
        50.0,
        1,
        50.0,
        clusterAndTypeAttributes(AdminMessageType.UPDATE_STORE));

    validateTehutiMetric(
        AdminConsumptionStats.AdminConsumptionTehutiMetricNameEnum.ADMIN_MESSAGE_START_PROCESSING_LATENCY_MS,
        "Avg",
        50.0);
    validateTehutiMetric(
        AdminConsumptionStats.AdminConsumptionTehutiMetricNameEnum.ADMIN_MESSAGE_START_PROCESSING_LATENCY_MS,
        "Max",
        50.0);
  }

  @Test
  public void testRecordCycleTime() {
    stats.recordAdminConsumptionCycleDurationMs(300.0);

    validateHistogram(
        AdminConsumptionStats.AdminConsumptionOtelMetricEntity.ADMIN_CONSUMPTION_MESSAGE_BATCH_PROCESSING_CYCLE_TIME
            .getMetricName(),
        300.0,
        300.0,
        1,
        300.0,
        clusterAttributes());

    validateTehutiMetric(
        AdminConsumptionStats.AdminConsumptionTehutiMetricNameEnum.ADMIN_CONSUMPTION_CYCLE_DURATION_MS,
        "Avg",
        300.0);
    validateTehutiMetric(
        AdminConsumptionStats.AdminConsumptionTehutiMetricNameEnum.ADMIN_CONSUMPTION_CYCLE_DURATION_MS,
        "Min",
        300.0);
    validateTehutiMetric(
        AdminConsumptionStats.AdminConsumptionTehutiMetricNameEnum.ADMIN_CONSUMPTION_CYCLE_DURATION_MS,
        "Max",
        300.0);
  }

  // Histogram with AdminMessageType tests (start-to-end processing time)

  @Test
  public void testRecordProcessingTimeByAdminMessageType() {
    stats.recordAdminMessageProcessLatency(150.0, AdminMessageType.UPDATE_STORE);

    validateHistogram(
        AdminConsumptionStats.AdminConsumptionOtelMetricEntity.ADMIN_CONSUMPTION_MESSAGE_PHASE_START_TO_END_PROCESSING_TIME
            .getMetricName(),
        150.0,
        150.0,
        1,
        150.0,
        clusterAndTypeAttributes(AdminMessageType.UPDATE_STORE));

    validateTehutiMetric(
        AdminConsumptionStats.AdminConsumptionTehutiMetricNameEnum.ADMIN_MESSAGE_PROCESS_LATENCY_MS,
        "Avg",
        150.0);
    validateTehutiMetric(
        AdminConsumptionStats.AdminConsumptionTehutiMetricNameEnum.ADMIN_MESSAGE_PROCESS_LATENCY_MS,
        "Max",
        150.0);
  }

  @Test
  public void testRecordProcessingTimeForDifferentTypes() {
    stats.recordAdminMessageProcessLatency(100.0, AdminMessageType.STORE_CREATION);
    stats.recordAdminMessageProcessLatency(200.0, AdminMessageType.DELETE_STORE);

    validateHistogram(
        AdminConsumptionStats.AdminConsumptionOtelMetricEntity.ADMIN_CONSUMPTION_MESSAGE_PHASE_START_TO_END_PROCESSING_TIME
            .getMetricName(),
        100.0,
        100.0,
        1,
        100.0,
        clusterAndTypeAttributes(AdminMessageType.STORE_CREATION));

    validateHistogram(
        AdminConsumptionStats.AdminConsumptionOtelMetricEntity.ADMIN_CONSUMPTION_MESSAGE_PHASE_START_TO_END_PROCESSING_TIME
            .getMetricName(),
        200.0,
        200.0,
        1,
        200.0,
        clusterAndTypeAttributes(AdminMessageType.DELETE_STORE));

    // Tehuti: both types share the same processLatencyMetric sensor, so Avg = (100+200)/2
    validateTehutiMetric(
        AdminConsumptionStats.AdminConsumptionTehutiMetricNameEnum.ADMIN_MESSAGE_PROCESS_LATENCY_MS,
        "Avg",
        150.0);
    validateTehutiMetric(
        AdminConsumptionStats.AdminConsumptionTehutiMetricNameEnum.ADMIN_MESSAGE_PROCESS_LATENCY_MS,
        "Max",
        200.0);
  }

  @Test
  public void testRecordAddVersionProcessingTime() {
    stats.recordAdminMessageProcessLatency(500.0, AdminMessageType.ADD_VERSION);

    validateHistogram(
        AdminConsumptionStats.AdminConsumptionOtelMetricEntity.ADMIN_CONSUMPTION_MESSAGE_PHASE_START_TO_END_PROCESSING_TIME
            .getMetricName(),
        500.0,
        500.0,
        1,
        500.0,
        clusterAndTypeAttributes(AdminMessageType.ADD_VERSION));

    // ADD_VERSION goes to the separate addVersionProcessLatencyMetric Tehuti sensor
    validateTehutiMetric(
        AdminConsumptionStats.AdminConsumptionTehutiMetricNameEnum.ADMIN_MESSAGE_ADD_VERSION_PROCESS_LATENCY_MS,
        "Avg",
        500.0);
    validateTehutiMetric(
        AdminConsumptionStats.AdminConsumptionTehutiMetricNameEnum.ADMIN_MESSAGE_ADD_VERSION_PROCESS_LATENCY_MS,
        "Max",
        500.0);
  }

  // Tehuti-only raw sensor test

  @Test
  public void testRecordAdminMessageTotalLatency() {
    stats.recordAdminMessageTotalLatency(250.0);

    String tehutiMetricName =
        AbstractVeniceStats.getSensorFullName(RESOURCE_NAME, "admin_message_total_latency_ms") + ".Avg";
    assertNotNull(metricsRepository.getMetric(tehutiMetricName), "Tehuti metric should exist: " + tehutiMetricName);
    assertEquals(metricsRepository.getMetric(tehutiMetricName).value(), 250.0);
  }

  // Async Gauge tests

  @Test
  public void testAsyncGaugePendingMessageCount() {
    stats.recordPendingAdminMessagesCount(42.0);

    validateGauge(
        AdminConsumptionStats.AdminConsumptionOtelMetricEntity.ADMIN_CONSUMPTION_MESSAGE_PENDING_COUNT.getMetricName(),
        42,
        clusterAttributes());

    validateTehutiMetric(
        AdminConsumptionStats.AdminConsumptionTehutiMetricNameEnum.PENDING_ADMIN_MESSAGES_COUNT,
        "Gauge",
        42.0);
  }

  @Test
  public void testAsyncGaugeStorePendingCount() {
    stats.recordStoresWithPendingAdminMessagesCount(7.0);

    validateGauge(
        AdminConsumptionStats.AdminConsumptionOtelMetricEntity.ADMIN_CONSUMPTION_STORE_PENDING_COUNT.getMetricName(),
        7,
        clusterAttributes());

    validateTehutiMetric(
        AdminConsumptionStats.AdminConsumptionTehutiMetricNameEnum.STORES_WITH_PENDING_ADMIN_MESSAGES_COUNT,
        "Gauge",
        7.0);
  }

  @Test
  public void testAsyncGaugeOffsetLag() {
    stats.setAdminConsumptionOffsetLag(1000L);

    validateGauge(
        AdminConsumptionStats.AdminConsumptionOtelMetricEntity.ADMIN_CONSUMPTION_CONSUMER_OFFSET_LAG.getMetricName(),
        1000,
        clusterAttributes());

    validateTehutiMetric(
        AdminConsumptionStats.AdminConsumptionTehutiMetricNameEnum.ADMIN_CONSUMPTION_OFFSET_LAG,
        "Gauge",
        1000.0);
  }

  @Test
  public void testAsyncGaugeCheckpointOffsetLag() {
    stats.setMaxAdminConsumptionOffsetLag(2000L);

    validateGauge(
        AdminConsumptionStats.AdminConsumptionOtelMetricEntity.ADMIN_CONSUMPTION_CONSUMER_CHECKPOINT_OFFSET_LAG
            .getMetricName(),
        2000,
        clusterAttributes());

    validateTehutiMetric(
        AdminConsumptionStats.AdminConsumptionTehutiMetricNameEnum.MAX_ADMIN_CONSUMPTION_OFFSET_LAG,
        "Gauge",
        2000.0);
  }

  // Standard tests

  @Test
  public void testNoNpeWhenOtelDisabled() {
    VeniceMetricsRepository disabledRepo = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX).setEmitOtelMetrics(false).build());
    AdminConsumptionStats disabledStats = new AdminConsumptionStats(disabledRepo, TEST_CLUSTER_NAME);

    // All methods should execute without NPE
    disabledStats.recordFailedAdminConsumption();
    disabledStats.recordFailedRetriableAdminConsumption();
    disabledStats.recordAdminTopicDIVErrorReportCount();
    disabledStats.recordAdminMessagesWithFutureProtocolVersionCount();
    disabledStats.recordAdminMessageMMLatency(10.0, AdminMessageType.UPDATE_STORE);
    disabledStats.recordAdminMessageDelegateLatency(10.0, AdminMessageType.UPDATE_STORE);
    disabledStats.recordAdminMessageStartProcessingLatency(10.0, AdminMessageType.UPDATE_STORE);
    disabledStats.recordAdminMessageProcessLatency(10.0, AdminMessageType.UPDATE_STORE);
    disabledStats.recordAdminMessageProcessLatency(10.0, AdminMessageType.ADD_VERSION);
    disabledStats.recordAdminConsumptionCycleDurationMs(10.0);
    disabledStats.recordAdminMessageTotalLatency(10.0);
    disabledStats.recordPendingAdminMessagesCount(5.0);
    disabledStats.recordStoresWithPendingAdminMessagesCount(3.0);
    disabledStats.setAdminConsumptionOffsetLag(100L);
    disabledStats.setMaxAdminConsumptionOffsetLag(200L);
  }

  @Test
  public void testNoNpeWhenPlainMetricsRepository() {
    MetricsRepository plainRepo = MetricsRepositoryUtils.createSingleThreadedMetricsRepository();
    AdminConsumptionStats plainStats = new AdminConsumptionStats(plainRepo, TEST_CLUSTER_NAME);

    plainStats.recordFailedAdminConsumption();
    plainStats.recordFailedRetriableAdminConsumption();
    plainStats.recordAdminTopicDIVErrorReportCount();
    plainStats.recordAdminMessagesWithFutureProtocolVersionCount();
    plainStats.recordAdminMessageMMLatency(10.0, AdminMessageType.UPDATE_STORE);
    plainStats.recordAdminMessageDelegateLatency(10.0, AdminMessageType.UPDATE_STORE);
    plainStats.recordAdminMessageStartProcessingLatency(10.0, AdminMessageType.UPDATE_STORE);
    plainStats.recordAdminMessageProcessLatency(10.0, AdminMessageType.UPDATE_STORE);
    plainStats.recordAdminMessageProcessLatency(10.0, AdminMessageType.ADD_VERSION);
    plainStats.recordAdminConsumptionCycleDurationMs(10.0);
    plainStats.recordAdminMessageTotalLatency(10.0);
    plainStats.recordPendingAdminMessagesCount(5.0);
    plainStats.recordStoresWithPendingAdminMessagesCount(3.0);
    plainStats.setAdminConsumptionOffsetLag(100L);
    plainStats.setMaxAdminConsumptionOffsetLag(200L);
  }

  private Attributes clusterAttributes() {
    return Attributes.builder().put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME).build();
  }

  private Attributes clusterAndTypeAttributes(AdminMessageType adminMessageType) {
    return Attributes.builder()
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
        .put(VENICE_ADMIN_MESSAGE_TYPE.getDimensionNameInDefaultFormat(), adminMessageType.getDimensionValue())
        .build();
  }

  private void validateCounter(String metricName, long expectedValue, Attributes expectedAttributes) {
    OpenTelemetryDataTestUtils.validateLongPointDataFromCounter(
        inMemoryMetricReader,
        expectedValue,
        expectedAttributes,
        metricName,
        TEST_METRIC_PREFIX);
  }

  private void validateHistogram(
      String metricName,
      double expectedMin,
      double expectedMax,
      long expectedCount,
      double expectedSum,
      Attributes expectedAttributes) {
    OpenTelemetryDataTestUtils.validateHistogramPointData(
        inMemoryMetricReader,
        expectedMin,
        expectedMax,
        expectedCount,
        expectedSum,
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

  private void validateTehutiMetric(
      AdminConsumptionStats.AdminConsumptionTehutiMetricNameEnum tehutiEnum,
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
