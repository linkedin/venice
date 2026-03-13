package com.linkedin.davinci.stats;

import static com.linkedin.davinci.stats.KafkaConsumerServiceOtelMetricEntity.ORPHAN_TOPIC_PARTITION_COUNT;
import static com.linkedin.davinci.stats.KafkaConsumerServiceOtelMetricEntity.PARTITION_ASSIGNMENT_COUNT;
import static com.linkedin.davinci.stats.KafkaConsumerServiceOtelMetricEntity.POLL_BYTES;
import static com.linkedin.davinci.stats.KafkaConsumerServiceOtelMetricEntity.POLL_COUNT;
import static com.linkedin.davinci.stats.KafkaConsumerServiceOtelMetricEntity.POLL_ERROR_COUNT;
import static com.linkedin.davinci.stats.KafkaConsumerServiceOtelMetricEntity.POLL_NON_EMPTY_COUNT;
import static com.linkedin.davinci.stats.KafkaConsumerServiceOtelMetricEntity.POLL_RECORD_COUNT;
import static com.linkedin.davinci.stats.KafkaConsumerServiceOtelMetricEntity.POLL_TIME;
import static com.linkedin.davinci.stats.KafkaConsumerServiceOtelMetricEntity.POLL_TIME_SINCE_LAST_SUCCESS;
import static com.linkedin.davinci.stats.KafkaConsumerServiceOtelMetricEntity.POOL_ACTION_TIME;
import static com.linkedin.davinci.stats.KafkaConsumerServiceOtelMetricEntity.PRODUCE_TO_WRITE_BUFFER_TIME;
import static com.linkedin.davinci.stats.KafkaConsumerServiceOtelMetricEntity.TOPIC_DETECTED_DELETED_COUNT;
import static com.linkedin.davinci.stats.ServerMetricEntity.SERVER_METRIC_ENTITIES;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CONSUMER_POOL_ACTION;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceConsumerPoolAction;
import com.linkedin.venice.utils.OpenTelemetryDataTestUtils;
import com.linkedin.venice.utils.SystemTime;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.metrics.data.ExponentialHistogramPointData;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.Metric;
import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.AsyncGauge;
import java.util.Collection;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class KafkaConsumerServiceStatsOtelTest {
  private static final String TEST_METRIC_PREFIX = "server";
  private static final String TEST_CLUSTER_NAME = "test-cluster";
  private static final String TEST_STORE_NAME = "test-store";
  private static final String TOTAL_STORE_NAME = "total.kafka_consumer_service_for_test-region";
  /** After AbstractVeniceStats sanitization: dots → underscores */
  private static final String TOTAL_STATS_NAME = "total_kafka_consumer_service_for_test-region";

  // Dedicated AsyncGaugeExecutor to avoid contention with the shared DEFAULT_ASYNC_GAUGE_EXECUTOR.
  // The static default executor gets permanently shut down when any MetricsRepository.close() triggers
  // its closure, causing all subsequent tests' AsyncGauge.measure() to return 0.0 (cached default).
  private AsyncGauge.AsyncGaugeExecutor asyncGaugeExecutor;
  private InMemoryMetricReader inMemoryMetricReader;
  private VeniceMetricsRepository metricsRepository;

  /** Total stats instance — receives total-only OTel metrics */
  private KafkaConsumerServiceStats totalStats;

  /** Per-store stats instance — receives per-store OTel metrics */
  private KafkaConsumerServiceStats perStoreStats;

  @BeforeMethod
  public void setUp() {
    asyncGaugeExecutor = new AsyncGauge.AsyncGaugeExecutor.Builder().build();
    inMemoryMetricReader = InMemoryMetricReader.create();
    metricsRepository = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX)
            .setMetricEntities(SERVER_METRIC_ENTITIES)
            .setEmitOtelMetrics(true)
            .setOtelAdditionalMetricsReader(inMemoryMetricReader)
            .setTehutiMetricConfig(new MetricConfig(asyncGaugeExecutor))
            .build());

    // Create total stats (totalStats=null means this IS the total instance)
    totalStats = new KafkaConsumerServiceStats(
        metricsRepository,
        TOTAL_STORE_NAME,
        () -> 42L,
        null,
        SystemTime.INSTANCE,
        TEST_CLUSTER_NAME);

    // Create per-store stats (totalStats!=null means this is a per-store instance)
    perStoreStats = new KafkaConsumerServiceStats(
        metricsRepository,
        TEST_STORE_NAME,
        () -> 42L,
        totalStats,
        SystemTime.INSTANCE,
        TEST_CLUSTER_NAME);
  }

  @AfterMethod
  public void tearDown() throws Exception {
    if (metricsRepository != null) {
      metricsRepository.close();
    }
    if (asyncGaugeExecutor != null) {
      asyncGaugeExecutor.close();
    }
  }

  // Per-store metrics — Joint API with parent propagation

  @Test
  public void testRecordByteSizePerPoll() {
    perStoreStats.recordByteSizePerPoll(1024);
    perStoreStats.recordByteSizePerPoll(2048);

    // OTel: MIN_MAX_COUNT_SUM histogram with per-store attributes
    OpenTelemetryDataTestUtils.validateHistogramPointData(
        inMemoryMetricReader,
        1024,
        2048,
        2,
        3072,
        buildPerStoreAttributes(),
        POLL_BYTES.getMetricName(),
        TEST_METRIC_PREFIX);

    // Per-store Tehuti: Min=1024, Max=2048
    assertEquals(getTehutiMetricValue(TEST_STORE_NAME, "bytes_per_poll", "Min"), 1024.0);
    assertEquals(getTehutiMetricValue(TEST_STORE_NAME, "bytes_per_poll", "Max"), 2048.0);

    // Total Tehuti: same values via parent propagation — Min=1024, Max=2048
    assertEquals(getTehutiMetricValue(TOTAL_STATS_NAME, "bytes_per_poll", "Min"), 1024.0);
    assertEquals(getTehutiMetricValue(TOTAL_STATS_NAME, "bytes_per_poll", "Max"), 2048.0);
  }

  @Test
  public void testRecordPollResultNum() {
    perStoreStats.recordPollResultNum(100);
    perStoreStats.recordPollResultNum(200);

    // OTel: MIN_MAX_COUNT_SUM histogram with per-store attributes
    OpenTelemetryDataTestUtils.validateHistogramPointData(
        inMemoryMetricReader,
        100,
        200,
        2,
        300,
        buildPerStoreAttributes(),
        POLL_RECORD_COUNT.getMetricName(),
        TEST_METRIC_PREFIX);

    // Per-store Tehuti: Avg=150, Min=100
    assertEquals(getTehutiMetricValue(TEST_STORE_NAME, "consumer_poll_result_num", "Avg"), 150.0);
    assertEquals(getTehutiMetricValue(TEST_STORE_NAME, "consumer_poll_result_num", "Min"), 100.0);

    // Total Tehuti: same values via parent propagation
    assertEquals(getTehutiMetricValue(TOTAL_STATS_NAME, "consumer_poll_result_num", "Avg"), 150.0);
    assertEquals(getTehutiMetricValue(TOTAL_STATS_NAME, "consumer_poll_result_num", "Min"), 100.0);
  }

  // Double-counting prevention — OTel isolation between total and per-store instances

  /**
   * Verifies that per-store OTel recordings do NOT produce data points with total attributes.
   * Tehuti parent propagation IS expected (per-store Tehuti sensor propagates to total Tehuti sensor),
   * but OTel has no parent concept — each instance records only with its own attributes.
   */
  @Test
  public void testPerStoreRecordingDoesNotProduceOtelDataWithTotalAttributes() {
    perStoreStats.recordByteSizePerPoll(1024);
    perStoreStats.recordPollResultNum(100);

    Collection<MetricData> metrics = inMemoryMetricReader.collectAllMetrics();
    Attributes totalAttrs = buildTotalAttributes();
    Attributes perStoreAttrs = buildPerStoreAttributes();

    // Per-store OTel data SHOULD exist
    HistogramPointData bytesPerStore = OpenTelemetryDataTestUtils
        .getHistogramPointData(metrics, POLL_BYTES.getMetricName(), TEST_METRIC_PREFIX, perStoreAttrs);
    assertNotNull(bytesPerStore, "Per-store OTel data should exist for poll bytes");

    HistogramPointData recordCountPerStore = OpenTelemetryDataTestUtils
        .getHistogramPointData(metrics, POLL_RECORD_COUNT.getMetricName(), TEST_METRIC_PREFIX, perStoreAttrs);
    assertNotNull(recordCountPerStore, "Per-store OTel data should exist for poll record count");

    // Total OTel data should NOT exist — only Tehuti propagates via parent sensor, not OTel
    HistogramPointData bytesTotal = OpenTelemetryDataTestUtils
        .getHistogramPointData(metrics, POLL_BYTES.getMetricName(), TEST_METRIC_PREFIX, totalAttrs);
    assertNull(bytesTotal, "Per-store recording should not produce OTel data with total attributes");

    HistogramPointData recordCountTotal = OpenTelemetryDataTestUtils
        .getHistogramPointData(metrics, POLL_RECORD_COUNT.getMetricName(), TEST_METRIC_PREFIX, totalAttrs);
    assertNull(recordCountTotal, "Per-store recording should not produce OTel data with total attributes");

    // Tehuti total SHOULD have data via parent propagation (this is the expected design)
    assertEquals(getTehutiMetricValue(TOTAL_STATS_NAME, "bytes_per_poll", "Max"), 1024.0);
    assertEquals(getTehutiMetricValue(TOTAL_STATS_NAME, "consumer_poll_result_num", "Avg"), 100.0);
  }

  /**
   * Verifies that total-only OTel recordings do NOT produce data points with per-store attributes.
   * Covers synchronous metric types: HISTOGRAM, COUNTER, and MIN_MAX_COUNT_SUM.
   * Async metrics (POLL_COUNT, POLL_NON_EMPTY_COUNT) are excluded because async callbacks
   * register at construction time and may report zero-valued data points.
   */
  @Test
  public void testTotalRecordingDoesNotProduceOtelDataWithPerStoreAttributes() {
    totalStats.recordPollRequestLatency(50.0);
    totalStats.recordPollError();
    totalStats.recordConsumerRecordsProducingToWriterBufferLatency(10.0);
    totalStats.recordDetectedDeletedTopicNum(2);
    totalStats.recordDetectedNoRunningIngestionTopicPartitionNum(1);
    totalStats.recordConsumerIdleTime(500.0);
    totalStats.recordDelegateSubscribeLatency(15.0);

    Collection<MetricData> metrics = inMemoryMetricReader.collectAllMetrics();
    Attributes perStoreAttrs = buildPerStoreAttributes();

    // HISTOGRAM metrics: no per-store data points
    ExponentialHistogramPointData pollTimePerStore = OpenTelemetryDataTestUtils
        .getExponentialHistogramPointData(metrics, POLL_TIME.getMetricName(), TEST_METRIC_PREFIX, perStoreAttrs);
    assertNull(pollTimePerStore, "Total-only poll time should not produce per-store OTel data");

    ExponentialHistogramPointData writeBufferPerStore = OpenTelemetryDataTestUtils.getExponentialHistogramPointData(
        metrics,
        PRODUCE_TO_WRITE_BUFFER_TIME.getMetricName(),
        TEST_METRIC_PREFIX,
        perStoreAttrs);
    assertNull(writeBufferPerStore, "Total-only write buffer time should not produce per-store OTel data");

    // HISTOGRAM with enum dimension: no per-store data points (need per-store + action attributes)
    Attributes perStoreSubscribeAttrs = buildPerStoreAttributes().toBuilder()
        .put(
            VENICE_CONSUMER_POOL_ACTION.getDimensionNameInDefaultFormat(),
            VeniceConsumerPoolAction.SUBSCRIBE.getDimensionValue())
        .build();
    ExponentialHistogramPointData subscribePerStore = OpenTelemetryDataTestUtils.getExponentialHistogramPointData(
        metrics,
        POOL_ACTION_TIME.getMetricName(),
        TEST_METRIC_PREFIX,
        perStoreSubscribeAttrs);
    assertNull(subscribePerStore, "Total-only subscribe latency should not produce per-store OTel data");

    // COUNTER metrics: no per-store data points
    OpenTelemetryDataTestUtils
        .assertNoLongSumDataForAttributes(metrics, POLL_ERROR_COUNT.getMetricName(), TEST_METRIC_PREFIX, perStoreAttrs);
    OpenTelemetryDataTestUtils.assertNoLongSumDataForAttributes(
        metrics,
        TOPIC_DETECTED_DELETED_COUNT.getMetricName(),
        TEST_METRIC_PREFIX,
        perStoreAttrs);
    OpenTelemetryDataTestUtils.assertNoLongSumDataForAttributes(
        metrics,
        ORPHAN_TOPIC_PARTITION_COUNT.getMetricName(),
        TEST_METRIC_PREFIX,
        perStoreAttrs);

    // MIN_MAX_COUNT_SUM metric: no per-store data point
    HistogramPointData idleTimePerStore = OpenTelemetryDataTestUtils.getHistogramPointData(
        metrics,
        POLL_TIME_SINCE_LAST_SUCCESS.getMetricName(),
        TEST_METRIC_PREFIX,
        perStoreAttrs);
    assertNull(idleTimePerStore, "Total-only idle time should not produce per-store OTel data");
  }

  // Total-only metrics — Joint API

  @Test
  public void testRecordPollRequestLatency() {
    totalStats.recordPollRequestLatency(50.0);
    totalStats.recordPollRequestLatency(100.0);

    // OTel: ASYNC_COUNTER_FOR_HIGH_PERF_CASES for poll count
    OpenTelemetryDataTestUtils.validateObservableCounterValue(
        inMemoryMetricReader,
        2,
        buildTotalAttributes(),
        POLL_COUNT.getMetricName(),
        TEST_METRIC_PREFIX);

    // OTel: HISTOGRAM for poll time
    OpenTelemetryDataTestUtils.validateExponentialHistogramPointData(
        inMemoryMetricReader,
        50.0,
        100.0,
        2,
        150.0,
        buildTotalAttributes(),
        POLL_TIME.getMetricName(),
        TEST_METRIC_PREFIX);

    // Tehuti: poll time Avg=75, Max=100
    assertEquals(getTehutiMetricValue(TOTAL_STATS_NAME, "consumer_poll_request_latency", "Avg"), 75.0);
    assertEquals(getTehutiMetricValue(TOTAL_STATS_NAME, "consumer_poll_request_latency", "Max"), 100.0);

    // Tehuti: poll count sensor exists (LongAdderRateGauge uses "Rate" suffix; rate value is
    // time-dependent so only existence is asserted — VeniceMetricsRepository uses SystemTime)
    assertNotNull(
        metricsRepository.getMetric("." + TOTAL_STATS_NAME + "--consumer_poll_request.Rate"),
        "Rate Tehuti sensor should be registered for poll count");
  }

  @Test
  public void testRecordNonZeroPollResultNum() {
    totalStats.recordNonZeroPollResultNum(5);
    totalStats.recordNonZeroPollResultNum(3);

    // OTel: ASYNC_COUNTER_FOR_HIGH_PERF_CASES: 5 + 3 = 8
    OpenTelemetryDataTestUtils.validateObservableCounterValue(
        inMemoryMetricReader,
        8,
        buildTotalAttributes(),
        POLL_NON_EMPTY_COUNT.getMetricName(),
        TEST_METRIC_PREFIX);

    // Tehuti: sensor exists (LongAdderRateGauge uses "Rate" suffix; rate value is
    // time-dependent so only existence is asserted — VeniceMetricsRepository uses SystemTime)
    assertNotNull(
        metricsRepository.getMetric("." + TOTAL_STATS_NAME + "--consumer_poll_non_zero_result_num.Rate"),
        "Rate Tehuti sensor should be registered for non-empty poll count");
  }

  @Test
  public void testRecordPollError() {
    totalStats.recordPollError();
    totalStats.recordPollError();
    totalStats.recordPollError();

    // OTel: COUNTER = 3
    validateCounter(POLL_ERROR_COUNT.getMetricName(), 3);

    // Tehuti: OccurrenceRate sensor should exist (rate value is time-dependent, so just check existence)
    assertNotNull(
        metricsRepository.getMetric("." + TOTAL_STATS_NAME + "--consumer_poll_error.OccurrenceRate"),
        "OccurrenceRate Tehuti sensor should be registered for poll error");
  }

  @Test
  public void testRecordConsumerRecordsProducingToWriterBufferLatency() {
    totalStats.recordConsumerRecordsProducingToWriterBufferLatency(10.0);
    totalStats.recordConsumerRecordsProducingToWriterBufferLatency(30.0);

    // OTel: HISTOGRAM
    OpenTelemetryDataTestUtils.validateExponentialHistogramPointData(
        inMemoryMetricReader,
        10.0,
        30.0,
        2,
        40.0,
        buildTotalAttributes(),
        PRODUCE_TO_WRITE_BUFFER_TIME.getMetricName(),
        TEST_METRIC_PREFIX);

    // Tehuti: Avg=20, Max=30
    assertEquals(
        getTehutiMetricValue(TOTAL_STATS_NAME, "consumer_records_producing_to_write_buffer_latency", "Avg"),
        20.0);
    assertEquals(
        getTehutiMetricValue(TOTAL_STATS_NAME, "consumer_records_producing_to_write_buffer_latency", "Max"),
        30.0);
  }

  @Test
  public void testRecordDetectedDeletedTopicNum() {
    totalStats.recordDetectedDeletedTopicNum(2);
    totalStats.recordDetectedDeletedTopicNum(3);

    // OTel: COUNTER = 5
    validateCounter(TOPIC_DETECTED_DELETED_COUNT.getMetricName(), 5);

    // Tehuti: Total = 5
    assertEquals(getTehutiMetricValue(TOTAL_STATS_NAME, "detected_deleted_topic_num", "Total"), 5.0);
  }

  @Test
  public void testRecordDetectedNoRunningIngestionTopicPartitionNum() {
    totalStats.recordDetectedNoRunningIngestionTopicPartitionNum(1);

    // OTel: COUNTER = 1
    validateCounter(ORPHAN_TOPIC_PARTITION_COUNT.getMetricName(), 1);

    // Tehuti: Total = 1
    assertEquals(
        getTehutiMetricValue(TOTAL_STATS_NAME, "detected_no_running_ingestion_topic_partition_num", "Total"),
        1.0);
  }

  @Test
  public void testRecordDelegateSubscribeLatency() {
    totalStats.recordDelegateSubscribeLatency(15.0);

    // OTel: HISTOGRAM with SUBSCRIBE dimension
    Attributes expectedAttributes = buildTotalAttributesWithAction(VeniceConsumerPoolAction.SUBSCRIBE);
    OpenTelemetryDataTestUtils.validateExponentialHistogramPointData(
        inMemoryMetricReader,
        15.0,
        15.0,
        1,
        15.0,
        expectedAttributes,
        POOL_ACTION_TIME.getMetricName(),
        TEST_METRIC_PREFIX);

    // Tehuti: Avg=15, Max=15
    assertEquals(getTehutiMetricValue(TOTAL_STATS_NAME, "delegate_subscribe_latency", "Avg"), 15.0);
    assertEquals(getTehutiMetricValue(TOTAL_STATS_NAME, "delegate_subscribe_latency", "Max"), 15.0);
  }

  @Test
  public void testRecordUpdateCurrentAssignmentLatency() {
    totalStats.recordUpdateCurrentAssignmentLatency(25.0);

    // OTel: HISTOGRAM with UPDATE_ASSIGNMENT dimension
    Attributes expectedAttributes = buildTotalAttributesWithAction(VeniceConsumerPoolAction.UPDATE_ASSIGNMENT);
    OpenTelemetryDataTestUtils.validateExponentialHistogramPointData(
        inMemoryMetricReader,
        25.0,
        25.0,
        1,
        25.0,
        expectedAttributes,
        POOL_ACTION_TIME.getMetricName(),
        TEST_METRIC_PREFIX);

    // Tehuti: Avg=25, Max=25
    assertEquals(getTehutiMetricValue(TOTAL_STATS_NAME, "update_current_assignment_latency", "Avg"), 25.0);
    assertEquals(getTehutiMetricValue(TOTAL_STATS_NAME, "update_current_assignment_latency", "Max"), 25.0);
  }

  @Test
  public void testSubscribeAndUpdateAssignmentAreIndependent() {
    totalStats.recordDelegateSubscribeLatency(10.0);
    totalStats.recordDelegateSubscribeLatency(20.0);
    totalStats.recordUpdateCurrentAssignmentLatency(50.0);

    // OTel: Subscribe min=10, max=20, count=2, sum=30
    OpenTelemetryDataTestUtils.validateExponentialHistogramPointData(
        inMemoryMetricReader,
        10.0,
        20.0,
        2,
        30.0,
        buildTotalAttributesWithAction(VeniceConsumerPoolAction.SUBSCRIBE),
        POOL_ACTION_TIME.getMetricName(),
        TEST_METRIC_PREFIX);

    // OTel: Update assignment min=50, max=50, count=1, sum=50
    OpenTelemetryDataTestUtils.validateExponentialHistogramPointData(
        inMemoryMetricReader,
        50.0,
        50.0,
        1,
        50.0,
        buildTotalAttributesWithAction(VeniceConsumerPoolAction.UPDATE_ASSIGNMENT),
        POOL_ACTION_TIME.getMetricName(),
        TEST_METRIC_PREFIX);

    // Tehuti: independent sensors with correct values
    assertEquals(getTehutiMetricValue(TOTAL_STATS_NAME, "delegate_subscribe_latency", "Max"), 20.0);
    assertEquals(getTehutiMetricValue(TOTAL_STATS_NAME, "update_current_assignment_latency", "Max"), 50.0);
  }

  @Test
  public void testRecordConsumerIdleTime() {
    totalStats.recordConsumerIdleTime(500.0);
    totalStats.recordConsumerIdleTime(200.0);

    // OTel: MIN_MAX_COUNT_SUM histogram: min=200, max=500, count=2, sum=700
    OpenTelemetryDataTestUtils.validateHistogramPointData(
        inMemoryMetricReader,
        200,
        500,
        2,
        700,
        buildTotalAttributes(),
        POLL_TIME_SINCE_LAST_SUCCESS.getMetricName(),
        TEST_METRIC_PREFIX);

    // Tehuti: Max = 500
    assertEquals(getTehutiMetricValue(TOTAL_STATS_NAME, "idle_time", "Max"), 500.0);
  }

  // ASYNC_COUNTER per-store suppression: per-store instances have totalOnlyOtelRepo=null,
  // so no OTel callbacks are registered. Per-store Tehuti shares total's sensor.

  /**
   * Verifies that ASYNC_COUNTER metrics (POLL_COUNT, POLL_NON_EMPTY_COUNT) do NOT register
   * OTel callbacks on per-store instances. Per-store instances have totalOnlyOtelRepo=null.
   */
  @Test
  public void testAsyncCounterPerStoreOtelSuppression() {
    // Record via per-store instance — this goes to shared Tehuti sensor but should NOT create OTel data
    perStoreStats.recordPollRequestLatency(10.0);
    perStoreStats.recordNonZeroPollResultNum(5);

    Collection<MetricData> metrics = inMemoryMetricReader.collectAllMetrics();
    Attributes perStoreAttrs = buildPerStoreAttributes();

    // No per-store OTel data for ASYNC_COUNTER metrics
    OpenTelemetryDataTestUtils
        .assertNoLongSumDataForAttributes(metrics, POLL_COUNT.getMetricName(), TEST_METRIC_PREFIX, perStoreAttrs);
    OpenTelemetryDataTestUtils.assertNoLongSumDataForAttributes(
        metrics,
        POLL_NON_EMPTY_COUNT.getMetricName(),
        TEST_METRIC_PREFIX,
        perStoreAttrs);
  }

  /**
   * Verifies that per-store ASYNC_COUNTER recordings share the total's Tehuti sensor.
   * Recording via perStoreStats should increment the total's LongAdderRateGauge sensor.
   */
  @Test
  public void testAsyncCounterTehutiSensorSharing() {
    // Record via per-store instance
    perStoreStats.recordPollRequestLatency(10.0);
    perStoreStats.recordPollRequestLatency(20.0);

    // Total's Tehuti sensor should exist (shared by per-store instance)
    assertNotNull(
        metricsRepository.getMetric("." + TOTAL_STATS_NAME + "--consumer_poll_request.Rate"),
        "Per-store recording should share total's Tehuti sensor for poll count");

    // Per-store should NOT have its own separate sensor
    assertNull(
        metricsRepository.getMetric("." + TEST_STORE_NAME + "--consumer_poll_request.Rate"),
        "Per-store instance should not have its own Tehuti sensor for poll count");
  }

  // Tehuti-only async gauge (OTel intentionally omitted — redundant with POLL_TIME_SINCE_LAST_SUCCESS histogram)

  @Test
  public void testAsyncGaugeMaxElapsedTimeSinceLastPoll() {
    // The supplier returns 42L (set in setUp). No OTel metric — only Tehuti AsyncGauge.
    // Uses a dedicated AsyncGaugeExecutor (not the shared static DEFAULT_ASYNC_GAUGE_EXECUTOR)
    // to avoid stale thread pool shutdown from prior test teardowns.
    assertEquals(getTehutiMetricValue(TOTAL_STATS_NAME, "max_elapsed_time_since_last_successful_poll", "Gauge"), 42.0);
  }

  // Tehuti-only partition gauges (no OTel counterpart — OTel uses PARTITION_ASSIGNMENT_COUNT histogram)

  @Test
  public void testTehutiOnlyPartitionGauges() {
    totalStats.recordMinPartitionsPerConsumer(2);
    totalStats.recordMaxPartitionsPerConsumer(15);
    totalStats.recordAvgPartitionsPerConsumer(8);
    totalStats.recordSubscribedPartitionsNum(50);

    assertEquals(getTehutiMetricValue(TOTAL_STATS_NAME, "min_partitions_per_consumer", "Gauge"), 2.0);
    assertEquals(getTehutiMetricValue(TOTAL_STATS_NAME, "max_partitions_per_consumer", "Gauge"), 15.0);
    assertEquals(getTehutiMetricValue(TOTAL_STATS_NAME, "avg_partitions_per_consumer", "Gauge"), 8.0);
    assertEquals(getTehutiMetricValue(TOTAL_STATS_NAME, "subscribed_partitions_num", "Gauge"), 50.0);
  }

  // OTel-only metric

  @Test
  public void testRecordPartitionAssignmentForOtel() {
    totalStats.recordPartitionAssignmentForOtel(5);
    totalStats.recordPartitionAssignmentForOtel(10);
    totalStats.recordPartitionAssignmentForOtel(3);

    // OTel: MIN_MAX_COUNT_SUM histogram: min=3, max=10, count=3, sum=18
    OpenTelemetryDataTestUtils.validateHistogramPointData(
        inMemoryMetricReader,
        3,
        10,
        3,
        18,
        buildTotalAttributes(),
        PARTITION_ASSIGNMENT_COUNT.getMetricName(),
        TEST_METRIC_PREFIX);
  }

  // All metrics are created unconditionally (matching original Tehuti pattern).
  // Callers (AggKafkaConsumerServiceStats.recordTotal*) control which instance gets recorded to.

  @Test
  public void testTotalRecordingOfPerStoreMetrics() {
    totalStats.recordByteSizePerPoll(1024);
    totalStats.recordPollResultNum(100);

    OpenTelemetryDataTestUtils.validateHistogramPointData(
        inMemoryMetricReader,
        1024,
        1024,
        1,
        1024,
        buildTotalAttributes(),
        POLL_BYTES.getMetricName(),
        TEST_METRIC_PREFIX);

    OpenTelemetryDataTestUtils.validateHistogramPointData(
        inMemoryMetricReader,
        100,
        100,
        1,
        100,
        buildTotalAttributes(),
        POLL_RECORD_COUNT.getMetricName(),
        TEST_METRIC_PREFIX);

    assertEquals(getTehutiMetricValue(TOTAL_STATS_NAME, "bytes_per_poll", "Max"), 1024.0);
  }

  // NPE prevention tests

  @Test
  public void testNoNpeWhenOtelDisabled() {
    try (VeniceMetricsRepository disabledRepo = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX).setEmitOtelMetrics(false).build())) {
      assertAllMethodsSafeWithRepo(disabledRepo);
    }
  }

  @Test
  public void testNoNpeWhenPlainMetricsRepository() {
    assertAllMethodsSafeWithRepo(new MetricsRepository());
  }

  private void assertAllMethodsSafeWithRepo(MetricsRepository repo) {
    KafkaConsumerServiceStats safeTotalStats =
        new KafkaConsumerServiceStats(repo, "total.test-region", () -> 0L, null, SystemTime.INSTANCE, null);
    KafkaConsumerServiceStats safePerStoreStats =
        new KafkaConsumerServiceStats(repo, "test-store", () -> 0L, safeTotalStats, SystemTime.INSTANCE, null);

    // Total-only methods
    safeTotalStats.recordPollRequestLatency(10.0);
    safeTotalStats.recordNonZeroPollResultNum(1);
    safeTotalStats.recordPollError();
    safeTotalStats.recordConsumerRecordsProducingToWriterBufferLatency(5.0);
    safeTotalStats.recordDetectedDeletedTopicNum(1);
    safeTotalStats.recordDetectedNoRunningIngestionTopicPartitionNum(1);
    safeTotalStats.recordDelegateSubscribeLatency(5.0);
    safeTotalStats.recordUpdateCurrentAssignmentLatency(5.0);
    safeTotalStats.recordConsumerIdleTime(100.0);
    safeTotalStats.recordMinPartitionsPerConsumer(1);
    safeTotalStats.recordMaxPartitionsPerConsumer(10);
    safeTotalStats.recordAvgPartitionsPerConsumer(5);
    safeTotalStats.recordSubscribedPartitionsNum(50);
    safeTotalStats.recordPartitionAssignmentForOtel(5);

    // Per-store methods
    safePerStoreStats.recordByteSizePerPoll(1024);
    safePerStoreStats.recordPollResultNum(100);
  }

  // Helper methods

  private Attributes buildAttributes(String storeName) {
    return Attributes.builder()
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), storeName)
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
        .build();
  }

  private Attributes buildTotalAttributes() {
    return buildAttributes(TOTAL_STORE_NAME);
  }

  private Attributes buildPerStoreAttributes() {
    return buildAttributes(TEST_STORE_NAME);
  }

  private Attributes buildTotalAttributesWithAction(VeniceConsumerPoolAction action) {
    return buildAttributes(TOTAL_STORE_NAME).toBuilder()
        .put(VENICE_CONSUMER_POOL_ACTION.getDimensionNameInDefaultFormat(), action.getDimensionValue())
        .build();
  }

  private void validateCounter(String metricName, long expectedValue) {
    OpenTelemetryDataTestUtils.validateLongPointDataFromCounter(
        inMemoryMetricReader,
        expectedValue,
        buildTotalAttributes(),
        metricName,
        TEST_METRIC_PREFIX);
  }

  /**
   * Gets a Tehuti metric value using the standard naming convention:
   * {@code .{statsName}--{sensorName}.{statType}}
   */
  private double getTehutiMetricValue(String statsName, String sensorName, String statType) {
    String metricName = "." + statsName + "--" + sensorName + "." + statType;
    Metric metric = metricsRepository.getMetric(metricName);
    assertNotNull(metric, "Tehuti metric should exist: " + metricName);
    return metric.value();
  }
}
