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

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.LongAdderRateGauge;
import com.linkedin.venice.stats.OpenTelemetryMetricsSetup;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceConsumerPoolAction;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.AsyncMetricEntityState.TehutiSensorRegistrationFunction;
import com.linkedin.venice.stats.metrics.MetricEntityStateBase;
import com.linkedin.venice.stats.metrics.MetricEntityStateOneEnum;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnum;
import com.linkedin.venice.utils.Time;
import io.opentelemetry.api.common.Attributes;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.AsyncGauge;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Gauge;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.Min;
import io.tehuti.metrics.stats.OccurrenceRate;
import io.tehuti.metrics.stats.Total;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.function.LongSupplier;


/**
 * This class provides the stats for Kafka consumer service per region or per store.
 * Stats inside this class can either:
 * (1) Total only: The stat indicate total number of all the stores on this host per region.
 * (2) Total and Per store only: The stat is registered for each store on this host.
 */
public class KafkaConsumerServiceStats extends AbstractVeniceStats {
  /** Tehuti metric names for KafkaConsumerServiceStats sensors. */
  enum TehutiMetricName implements TehutiMetricNameEnum {
    BYTES_PER_POLL, CONSUMER_POLL_RESULT_NUM, CONSUMER_POLL_REQUEST, CONSUMER_POLL_NON_ZERO_RESULT_NUM,
    CONSUMER_POLL_REQUEST_LATENCY, CONSUMER_POLL_ERROR, CONSUMER_RECORDS_PRODUCING_TO_WRITE_BUFFER_LATENCY,
    DETECTED_DELETED_TOPIC_NUM, DETECTED_NO_RUNNING_INGESTION_TOPIC_PARTITION_NUM, DELEGATE_SUBSCRIBE_LATENCY,
    UPDATE_CURRENT_ASSIGNMENT_LATENCY, IDLE_TIME, MAX_ELAPSED_TIME_SINCE_LAST_SUCCESSFUL_POLL
  }

  // Tehuti-only sensors (no OTel counterpart). min/max/avg partition counts are superseded by the
  // OTel MIN_MAX_COUNT_SUM metric (partitionAssignmentCountOtel). subscribedPartitionsNum (total count)
  // has no direct OTel equivalent — the histogram sum provides the equivalent.
  private final Sensor maxPartitionsPerConsumer;
  private final Sensor minPartitionsPerConsumer;
  private final Sensor avgPartitionsPerConsumer;
  private final Sensor subscribedPartitionsNum;

  // Joint Tehuti+OTel metric state fields

  // Per-store joint metrics: per-store Tehuti sensor has parent propagation to total's Tehuti sensor
  private final MetricEntityStateBase pollBytesOtel;
  private final MetricEntityStateBase pollRecordCountOtel;

  // ASYNC_COUNTER total-only metrics: OTel callback only on total instance (per-store would register
  // redundant ObservableCounter callbacks that always report zero). Per-store Tehuti shares total's sensor.
  private final MetricEntityStateBase pollCountOtel;
  private final MetricEntityStateBase pollNonEmptyCountOtel;

  // Total-only metrics: OTel only on total instance (per-store gets totalOnlyOtelRepo=null).
  // Tehuti sensors are registered on all instances but only total's sensor receives recordings
  // (callers use aggStats.recordTotal*()).
  private final MetricEntityStateBase pollTimeOtel;
  private final MetricEntityStateBase pollErrorCountOtel;
  private final MetricEntityStateBase produceToWriteBufferTimeOtel;
  private final MetricEntityStateBase topicDetectedDeletedCountOtel;
  private final MetricEntityStateBase orphanTopicPartitionCountOtel;
  private final MetricEntityStateBase pollTimeSinceLastSuccessOtel;

  // Shared OTel instrument with VeniceConsumerPoolAction dimension (total-only)
  private final MetricEntityStateOneEnum<VeniceConsumerPoolAction> subscribeActionTimeOtel;
  private final MetricEntityStateOneEnum<VeniceConsumerPoolAction> updateAssignmentActionTimeOtel;

  // OTel-only, recorded only on the total instance from KafkaConsumerService.recordPartitionsPerConsumerSensor()
  private final MetricEntityStateBase partitionAssignmentCountOtel;

  public KafkaConsumerServiceStats(
      MetricsRepository metricsRepository,
      String storeName,
      LongSupplier getMaxElapsedTimeSinceLastPollInConsumerPool,
      KafkaConsumerServiceStats totalStats,
      Time time,
      String clusterName) {
    super(metricsRepository, storeName);

    // OTel setup — intentionally NOT calling .isTotalStats(isTotalStats()) here.
    // In the standard pattern (ServerHttpRequestStats, etc.), .isTotalStats(true) disables
    // OTel on the total instance because per-store instances emit all metrics and OTel
    // aggregates at query time. This class is different: only 2 metrics (POLL_BYTES,
    // POLL_RECORD_COUNT) are per-store; the remaining 11 are recorded ONLY on the total
    // instance (via AggKafkaConsumerServiceStats.recordTotal*()). Disabling OTel on total
    // would leave those 11 metrics with no OTel representation at all. Instead, we use
    // totalOnlyOtelRepo (null for per-store instances) to suppress OTel on per-store
    // instances for total-only metrics.
    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo otelData =
        OpenTelemetryMetricsSetup.builder(metricsRepository)
            .setStoreName(storeName)
            .setClusterName(clusterName)
            .build();
    VeniceOpenTelemetryMetricsRepository otelRepository = otelData.getOtelRepository();
    Map<VeniceMetricsDimensions, String> baseDimensionsMap = otelData.getBaseDimensionsMap();
    Attributes baseAttributes = otelData.getBaseAttributes();

    // Tehuti-only partition gauges (no OTel counterpart — OTel records raw per-consumer values
    // to PARTITION_ASSIGNMENT_COUNT histogram, which provides min/max/avg/count natively)
    minPartitionsPerConsumer = registerSensor("min_partitions_per_consumer", new Gauge());
    maxPartitionsPerConsumer = registerSensor("max_partitions_per_consumer", new Gauge());
    avgPartitionsPerConsumer = registerSensor("avg_partitions_per_consumer", new Gauge());
    // Tehuti-only: total subscribed partition count across all consumers. No OTel counterpart
    // because OTel's PARTITION_ASSIGNMENT_COUNT histogram sum provides the equivalent aggregate.
    subscribedPartitionsNum = registerSensor("subscribed_partitions_num", new Gauge());

    // Per-store metrics: on per-store instances, Tehuti sensor has parent propagation to total
    pollBytesOtel = MetricEntityStateBase.create(
        POLL_BYTES.getMetricEntity(),
        otelRepository,
        totalStats == null ? this::registerSensorIfAbsent : registerPerStoreAndTotal(totalStats.pollBytesOtel),
        TehutiMetricName.BYTES_PER_POLL,
        Arrays.asList(new Min(), new Max()),
        baseDimensionsMap,
        baseAttributes);

    pollRecordCountOtel = MetricEntityStateBase.create(
        POLL_RECORD_COUNT.getMetricEntity(),
        otelRepository,
        totalStats == null ? this::registerSensorIfAbsent : registerPerStoreAndTotal(totalStats.pollRecordCountOtel),
        TehutiMetricName.CONSUMER_POLL_RESULT_NUM,
        Arrays.asList(new Avg(), new Min()),
        baseDimensionsMap,
        baseAttributes);

    // Total-only OTel repository: null for per-store instances to avoid registering
    // unused OTel instruments. Total-only metrics are only recorded via aggStats.recordTotal*().
    VeniceOpenTelemetryMetricsRepository totalOnlyOtelRepo = totalStats == null ? otelRepository : null;

    // ASYNC_COUNTER metrics: OTel callback only on total instance to avoid registering
    // redundant per-store ObservableCounter callbacks that would always report zero.
    // Per-store Tehuti shares total's sensor (matching original registerOnlyTotalRate behavior).
    TehutiSensorRegistrationFunction pollCountTehutiReg =
        totalStats == null ? this::registerSensorIfAbsent : (name, stats) -> totalStats.pollCountOtel.getTehutiSensor();
    pollCountOtel = MetricEntityStateBase.create(
        POLL_COUNT.getMetricEntity(),
        totalOnlyOtelRepo,
        pollCountTehutiReg,
        TehutiMetricName.CONSUMER_POLL_REQUEST,
        Collections.singletonList(new LongAdderRateGauge(time)),
        baseDimensionsMap,
        baseAttributes);

    TehutiSensorRegistrationFunction pollNonEmptyTehutiReg = totalStats == null
        ? this::registerSensorIfAbsent
        : (name, stats) -> totalStats.pollNonEmptyCountOtel.getTehutiSensor();
    pollNonEmptyCountOtel = MetricEntityStateBase.create(
        POLL_NON_EMPTY_COUNT.getMetricEntity(),
        totalOnlyOtelRepo,
        pollNonEmptyTehutiReg,
        TehutiMetricName.CONSUMER_POLL_NON_ZERO_RESULT_NUM,
        Collections.singletonList(new LongAdderRateGauge(time)),
        baseDimensionsMap,
        baseAttributes);

    pollTimeOtel = MetricEntityStateBase.create(
        POLL_TIME.getMetricEntity(),
        totalOnlyOtelRepo,
        this::registerSensorIfAbsent,
        TehutiMetricName.CONSUMER_POLL_REQUEST_LATENCY,
        Arrays.asList(new Avg(), new Max()),
        baseDimensionsMap,
        baseAttributes);

    pollErrorCountOtel = MetricEntityStateBase.create(
        POLL_ERROR_COUNT.getMetricEntity(),
        totalOnlyOtelRepo,
        this::registerSensorIfAbsent,
        TehutiMetricName.CONSUMER_POLL_ERROR,
        Collections.singletonList(new OccurrenceRate()),
        baseDimensionsMap,
        baseAttributes);

    produceToWriteBufferTimeOtel = MetricEntityStateBase.create(
        PRODUCE_TO_WRITE_BUFFER_TIME.getMetricEntity(),
        totalOnlyOtelRepo,
        this::registerSensorIfAbsent,
        TehutiMetricName.CONSUMER_RECORDS_PRODUCING_TO_WRITE_BUFFER_LATENCY,
        Arrays.asList(new Avg(), new Max()),
        baseDimensionsMap,
        baseAttributes);

    topicDetectedDeletedCountOtel = MetricEntityStateBase.create(
        TOPIC_DETECTED_DELETED_COUNT.getMetricEntity(),
        totalOnlyOtelRepo,
        this::registerSensorIfAbsent,
        TehutiMetricName.DETECTED_DELETED_TOPIC_NUM,
        Collections.singletonList(new Total()),
        baseDimensionsMap,
        baseAttributes);

    orphanTopicPartitionCountOtel = MetricEntityStateBase.create(
        ORPHAN_TOPIC_PARTITION_COUNT.getMetricEntity(),
        totalOnlyOtelRepo,
        this::registerSensorIfAbsent,
        TehutiMetricName.DETECTED_NO_RUNNING_INGESTION_TOPIC_PARTITION_NUM,
        Collections.singletonList(new Total()),
        baseDimensionsMap,
        baseAttributes);

    pollTimeSinceLastSuccessOtel = MetricEntityStateBase.create(
        POLL_TIME_SINCE_LAST_SUCCESS.getMetricEntity(),
        totalOnlyOtelRepo,
        this::registerSensorIfAbsent,
        TehutiMetricName.IDLE_TIME,
        Collections.singletonList(new Max()),
        baseDimensionsMap,
        baseAttributes);

    // Shared OTel instrument for subscribe + update_assignment, each with its own Tehuti sensor
    subscribeActionTimeOtel = MetricEntityStateOneEnum.create(
        POOL_ACTION_TIME.getMetricEntity(),
        totalOnlyOtelRepo,
        this::registerSensorIfAbsent,
        TehutiMetricName.DELEGATE_SUBSCRIBE_LATENCY,
        Arrays.asList(new Avg(), new Max()),
        baseDimensionsMap,
        VeniceConsumerPoolAction.class);

    updateAssignmentActionTimeOtel = MetricEntityStateOneEnum.create(
        POOL_ACTION_TIME.getMetricEntity(),
        totalOnlyOtelRepo,
        this::registerSensorIfAbsent,
        TehutiMetricName.UPDATE_CURRENT_ASSIGNMENT_LATENCY,
        Arrays.asList(new Avg(), new Max()),
        baseDimensionsMap,
        VeniceConsumerPoolAction.class);

    // Tehuti-only async gauge: OTel intentionally omitted because this reads from the same source
    // method (getMaxElapsedTimeMSSinceLastPollInConsumerPool) that also records to the
    // pollTimeSinceLastSuccessOtel metric above. Having both would be redundant in OTel.
    registerSensor(
        new AsyncGauge(
            (ignored, ignored2) -> getMaxElapsedTimeSinceLastPollInConsumerPool.getAsLong(),
            TehutiMetricName.MAX_ELAPSED_TIME_SINCE_LAST_SUCCESSFUL_POLL.getMetricName()));

    // OTel-only partition assignment histogram (asymmetric: raw values -> OTel, pre-computed -> Tehuti).
    // Uses totalOnlyOtelRepo: only the total instance needs this instrument — per-store instances
    // never call recordPartitionAssignmentForOtel().
    partitionAssignmentCountOtel = MetricEntityStateBase
        .create(PARTITION_ASSIGNMENT_COUNT.getMetricEntity(), totalOnlyOtelRepo, baseDimensionsMap, baseAttributes);
  }

  // Recording methods

  /** Joint API — records to both Tehuti (Avg, Max) and OTel (HISTOGRAM) for poll time, plus poll count. */
  public void recordPollRequestLatency(double latency) {
    pollTimeOtel.record(latency);
    pollCountOtel.record(1);
  }

  /** Joint API — records to both Tehuti (Avg, Min) and OTel (MIN_MAX_COUNT_SUM). */
  public void recordPollResultNum(int count) {
    pollRecordCountOtel.record(count);
  }

  /** Joint API — records to both Tehuti (LongAdderRateGauge) and OTel (ASYNC_COUNTER_FOR_HIGH_PERF_CASES). */
  public void recordNonZeroPollResultNum(int count) {
    pollNonEmptyCountOtel.record(count);
  }

  /** Joint API — records to both Tehuti (Avg, Max) and OTel (HISTOGRAM). */
  public void recordConsumerRecordsProducingToWriterBufferLatency(double latency) {
    produceToWriteBufferTimeOtel.record(latency);
  }

  /** Joint API — records to both Tehuti (OccurrenceRate) and OTel (COUNTER). */
  public void recordPollError() {
    pollErrorCountOtel.record(1);
  }

  /** Joint API — records to both Tehuti (Total) and OTel (COUNTER). */
  public void recordDetectedDeletedTopicNum(int count) {
    topicDetectedDeletedCountOtel.record(count);
  }

  /** Joint API — records to both Tehuti (Total) and OTel (COUNTER). */
  public void recordDetectedNoRunningIngestionTopicPartitionNum(int count) {
    orphanTopicPartitionCountOtel.record(count);
  }

  /** Joint API — records to both Tehuti (Avg, Max) and OTel (HISTOGRAM) with SUBSCRIBE dimension. */
  public void recordDelegateSubscribeLatency(double value) {
    subscribeActionTimeOtel.record(value, VeniceConsumerPoolAction.SUBSCRIBE);
  }

  /** Joint API — records to both Tehuti (Avg, Max) and OTel (HISTOGRAM) with UPDATE_ASSIGNMENT dimension. */
  public void recordUpdateCurrentAssignmentLatency(double value) {
    updateAssignmentActionTimeOtel.record(value, VeniceConsumerPoolAction.UPDATE_ASSIGNMENT);
  }

  public void recordMinPartitionsPerConsumer(int count) {
    minPartitionsPerConsumer.record(count);
  }

  public void recordMaxPartitionsPerConsumer(int count) {
    maxPartitionsPerConsumer.record(count);
  }

  public void recordAvgPartitionsPerConsumer(int count) {
    avgPartitionsPerConsumer.record(count);
  }

  /** Joint API — records to both Tehuti (Min, Max) and OTel (MIN_MAX_COUNT_SUM). */
  public void recordByteSizePerPoll(double count) {
    pollBytesOtel.record(count);
  }

  /** Joint API — records to both Tehuti (Max) and OTel (MIN_MAX_COUNT_SUM). */
  public void recordConsumerIdleTime(double time) {
    pollTimeSinceLastSuccessOtel.record(time);
  }

  public void recordSubscribedPartitionsNum(int count) {
    subscribedPartitionsNum.record(count);
  }

  /**
   * Records a single per-consumer partition count to the OTel partition assignment histogram.
   * Called for each consumer in the pool from
   * {@link com.linkedin.davinci.kafka.consumer.KafkaConsumerService#recordPartitionsPerConsumerSensor()}.
   * Tehuti keeps 4 pre-computed gauge values; OTel records raw per-consumer values to a single
   * MIN_MAX_COUNT_SUM histogram.
   */
  public void recordPartitionAssignmentForOtel(int partitionCount) {
    partitionAssignmentCountOtel.record(partitionCount);
  }
}
