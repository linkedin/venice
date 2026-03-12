package com.linkedin.davinci.stats;

import static com.linkedin.davinci.stats.KafkaConsumerServiceOtelMetricEntity.CONSUMER_ACTION_TIME;
import static com.linkedin.davinci.stats.KafkaConsumerServiceOtelMetricEntity.PARTITION_ASSIGNMENT_COUNT;
import static com.linkedin.davinci.stats.KafkaConsumerServiceOtelMetricEntity.POLL_BYTES;
import static com.linkedin.davinci.stats.KafkaConsumerServiceOtelMetricEntity.POLL_COUNT;
import static com.linkedin.davinci.stats.KafkaConsumerServiceOtelMetricEntity.POLL_ERROR_COUNT;
import static com.linkedin.davinci.stats.KafkaConsumerServiceOtelMetricEntity.POLL_NON_EMPTY_COUNT;
import static com.linkedin.davinci.stats.KafkaConsumerServiceOtelMetricEntity.POLL_RECORD_COUNT;
import static com.linkedin.davinci.stats.KafkaConsumerServiceOtelMetricEntity.POLL_TIME;
import static com.linkedin.davinci.stats.KafkaConsumerServiceOtelMetricEntity.POLL_TIME_SINCE_LAST_SUCCESS;
import static com.linkedin.davinci.stats.KafkaConsumerServiceOtelMetricEntity.POOL_IDLE_TIME;
import static com.linkedin.davinci.stats.KafkaConsumerServiceOtelMetricEntity.PRODUCE_TO_WRITE_BUFFER_TIME;
import static com.linkedin.davinci.stats.KafkaConsumerServiceOtelMetricEntity.TOPIC_DELETED_COUNT;
import static com.linkedin.davinci.stats.KafkaConsumerServiceOtelMetricEntity.TOPIC_NO_INGESTION_COUNT;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.LongAdderRateGauge;
import com.linkedin.venice.stats.OpenTelemetryMetricsSetup;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceConsumerPoolAction;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.AsyncMetricEntityState.TehutiSensorRegistrationFunction;
import com.linkedin.venice.stats.metrics.AsyncMetricEntityStateBase;
import com.linkedin.venice.stats.metrics.MetricEntityState;
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
  /** Tehuti metric names for joint Tehuti+OTel API. */
  enum TehutiMetricName implements TehutiMetricNameEnum {
    BYTES_PER_POLL, CONSUMER_POLL_RESULT_NUM, CONSUMER_POLL_REQUEST, CONSUMER_POLL_NON_ZERO_RESULT_NUM,
    CONSUMER_POLL_REQUEST_LATENCY, CONSUMER_POLL_ERROR, CONSUMER_RECORDS_PRODUCING_TO_WRITE_BUFFER_LATENCY,
    DETECTED_DELETED_TOPIC_NUM, DETECTED_NO_RUNNING_INGESTION_TOPIC_PARTITION_NUM, DELEGATE_SUBSCRIBE_LATENCY,
    UPDATE_CURRENT_ASSIGNMENT_LATENCY, IDLE_TIME, MAX_ELAPSED_TIME_SINCE_LAST_SUCCESSFUL_POLL
  }

  // Tehuti-only sensors (no OTel counterpart — OTel uses raw histogram via partitionAssignmentCountOtel)
  private final Sensor maxPartitionsPerConsumer;
  private final Sensor minPartitionsPerConsumer;
  private final Sensor avgPartitionsPerConsumer;
  private final Sensor subscribedPartitionsNum;

  // Joint Tehuti+OTel metric state fields

  // Per-store joint metrics: per-store Tehuti sensor has parent propagation to total's Tehuti sensor
  private final MetricEntityStateBase pollBytesOtel;
  private final MetricEntityStateBase pollRecordCountOtel;

  // All remaining metrics are created on every instance (matching original Tehuti pattern).
  // Callers (AggKafkaConsumerServiceStats.recordTotal*) control which instance gets recorded to.
  private final MetricEntityStateBase pollCountOtel;
  private final MetricEntityStateBase pollTimeOtel;
  private final MetricEntityStateBase pollNonEmptyCountOtel;
  private final MetricEntityStateBase pollErrorCountOtel;
  private final MetricEntityStateBase produceToWriteBufferTimeOtel;
  private final MetricEntityStateBase topicDeletedCountOtel;
  private final MetricEntityStateBase topicNoIngestionCountOtel;
  private final MetricEntityStateBase poolIdleTimeOtel;

  // Shared OTel instrument with VeniceConsumerPoolAction dimension
  private final MetricEntityStateOneEnum<VeniceConsumerPoolAction> subscribeActionTimeOtel;
  private final MetricEntityStateOneEnum<VeniceConsumerPoolAction> updateAssignmentActionTimeOtel;

  // OTel-only, recorded from KafkaConsumerService.recordPartitionsPerConsumerSensor()
  private final MetricEntityStateBase partitionAssignmentCountOtel;

  public KafkaConsumerServiceStats(
      MetricsRepository metricsRepository,
      String storeName,
      LongSupplier getMaxElapsedTimeSinceLastPollInConsumerPool,
      KafkaConsumerServiceStats totalStats,
      Time time,
      String clusterName) {
    super(metricsRepository, storeName);

    // OTel setup — intentionally NOT calling .isTotalStats(true) because that disables OTel
    // entirely. Unlike per-store-aggregated classes, this class has metrics that are only
    // recorded on the total instance, so OTel must remain enabled on total.
    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo otelData =
        OpenTelemetryMetricsSetup.builder(metricsRepository)
            .setStoreName(storeName)
            .setClusterName(clusterName)
            .build();
    VeniceOpenTelemetryMetricsRepository otelRepository = otelData.getOtelRepository();
    Map<VeniceMetricsDimensions, String> baseDimensionsMap = otelData.getBaseDimensionsMap();
    Attributes baseAttributes = otelData.getBaseAttributes();

    // Tehuti-only partition gauges (no OTel counterpart — OTel uses raw histogram)
    minPartitionsPerConsumer = registerSensor("min_partitions_per_consumer", new Gauge());
    maxPartitionsPerConsumer = registerSensor("max_partitions_per_consumer", new Gauge());
    avgPartitionsPerConsumer = registerSensor("avg_partitions_per_consumer", new Gauge());
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

    // All remaining metrics are created unconditionally (matching original Tehuti pattern).
    // Callers (AggKafkaConsumerServiceStats.recordTotal*) control which instance gets recorded to.
    pollCountOtel = MetricEntityStateBase.create(
        POLL_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        TehutiMetricName.CONSUMER_POLL_REQUEST,
        Collections.singletonList(new LongAdderRateGauge(time)),
        baseDimensionsMap,
        baseAttributes);

    pollNonEmptyCountOtel = MetricEntityStateBase.create(
        POLL_NON_EMPTY_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        TehutiMetricName.CONSUMER_POLL_NON_ZERO_RESULT_NUM,
        Collections.singletonList(new LongAdderRateGauge(time)),
        baseDimensionsMap,
        baseAttributes);

    pollTimeOtel = MetricEntityStateBase.create(
        POLL_TIME.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        TehutiMetricName.CONSUMER_POLL_REQUEST_LATENCY,
        Arrays.asList(new Avg(), new Max()),
        baseDimensionsMap,
        baseAttributes);

    pollErrorCountOtel = MetricEntityStateBase.create(
        POLL_ERROR_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        TehutiMetricName.CONSUMER_POLL_ERROR,
        Collections.singletonList(new OccurrenceRate()),
        baseDimensionsMap,
        baseAttributes);

    produceToWriteBufferTimeOtel = MetricEntityStateBase.create(
        PRODUCE_TO_WRITE_BUFFER_TIME.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        TehutiMetricName.CONSUMER_RECORDS_PRODUCING_TO_WRITE_BUFFER_LATENCY,
        Arrays.asList(new Avg(), new Max()),
        baseDimensionsMap,
        baseAttributes);

    topicDeletedCountOtel = MetricEntityStateBase.create(
        TOPIC_DELETED_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        TehutiMetricName.DETECTED_DELETED_TOPIC_NUM,
        Collections.singletonList(new Total()),
        baseDimensionsMap,
        baseAttributes);

    topicNoIngestionCountOtel = MetricEntityStateBase.create(
        TOPIC_NO_INGESTION_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        TehutiMetricName.DETECTED_NO_RUNNING_INGESTION_TOPIC_PARTITION_NUM,
        Collections.singletonList(new Total()),
        baseDimensionsMap,
        baseAttributes);

    poolIdleTimeOtel = MetricEntityStateBase.create(
        POOL_IDLE_TIME.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        TehutiMetricName.IDLE_TIME,
        Collections.singletonList(new Max()),
        baseDimensionsMap,
        baseAttributes);

    // Shared OTel instrument for subscribe + update_assignment, each with its own Tehuti sensor
    subscribeActionTimeOtel = MetricEntityStateOneEnum.create(
        CONSUMER_ACTION_TIME.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        TehutiMetricName.DELEGATE_SUBSCRIBE_LATENCY,
        Arrays.asList(new Avg(), new Max()),
        baseDimensionsMap,
        VeniceConsumerPoolAction.class);

    updateAssignmentActionTimeOtel = MetricEntityStateOneEnum.create(
        CONSUMER_ACTION_TIME.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        TehutiMetricName.UPDATE_CURRENT_ASSIGNMENT_LATENCY,
        Arrays.asList(new Avg(), new Max()),
        baseDimensionsMap,
        VeniceConsumerPoolAction.class);

    // Joint async gauge for max elapsed time since last successful poll.
    // OTel callback only on total instance — per-store instances would produce misleading data
    // points (pool-wide value tagged with per-store attributes) and register unnecessary callbacks.
    AsyncMetricEntityStateBase.create(
        POLL_TIME_SINCE_LAST_SUCCESS.getMetricEntity(),
        totalStats == null ? otelRepository : null,
        this::registerSensorIfAbsent,
        TehutiMetricName.MAX_ELAPSED_TIME_SINCE_LAST_SUCCESSFUL_POLL,
        Collections.singletonList(
            new AsyncGauge(
                (ignored, ignored2) -> getMaxElapsedTimeSinceLastPollInConsumerPool.getAsLong(),
                "max_elapsed_time_since_last_successful_poll")),
        baseDimensionsMap,
        baseAttributes,
        getMaxElapsedTimeSinceLastPollInConsumerPool);

    // OTel-only partition assignment histogram (asymmetric: raw values -> OTel, pre-computed -> Tehuti)
    partitionAssignmentCountOtel = MetricEntityStateBase
        .create(PARTITION_ASSIGNMENT_COUNT.getMetricEntity(), otelRepository, baseDimensionsMap, baseAttributes);
  }

  /** Returns a sensor registration function that propagates per-store recordings to the total's Tehuti sensor. */
  private TehutiSensorRegistrationFunction registerPerStoreAndTotal(MetricEntityState totalMetric) {
    Sensor parentSensor = totalMetric != null ? totalMetric.getTehutiSensor() : null;
    Sensor[] parents = parentSensor != null ? new Sensor[] { parentSensor } : null;
    return (name, stats) -> registerSensor(name, parents, stats);
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
    topicDeletedCountOtel.record(count);
  }

  /** Joint API — records to both Tehuti (Total) and OTel (COUNTER). */
  public void recordDetectedNoRunningIngestionTopicPartitionNum(int count) {
    topicNoIngestionCountOtel.record(count);
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

  /** Joint API — records to both Tehuti (Max) and OTel (GAUGE). */
  public void recordConsumerIdleTime(double time) {
    poolIdleTimeOtel.record(time);
  }

  public void recordSubscribedPartitionsNum(int count) {
    subscribedPartitionsNum.record(count);
  }

  /**
   * Records a single per-consumer partition count to the OTel partition assignment histogram.
   * Called for each consumer in the pool from {@link KafkaConsumerService#recordPartitionsPerConsumerSensor()}.
   * Tehuti keeps 4 pre-computed gauge values; OTel records raw per-consumer values to a single
   * MIN_MAX_COUNT_SUM histogram.
   */
  public void recordPartitionAssignmentForOtel(int partitionCount) {
    partitionAssignmentCountOtel.record(partitionCount);
  }
}
