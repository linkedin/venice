package com.linkedin.davinci.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.LongAdderRateGauge;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.lazy.Lazy;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.AsyncGauge;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Gauge;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.Min;
import io.tehuti.metrics.stats.OccurrenceRate;
import io.tehuti.metrics.stats.Total;
import java.util.function.LongSupplier;


/**
 * This class provides the stats for Kafka consumer service per region or per store.
 * Stats inside this class can either:
 * (1) Total only: The stat indicate total number of all the stores on this host per region.
 * (2) Total and Per store only: The stat is registered for each store on this host.
 */

public class KafkaConsumerServiceStats extends AbstractVeniceStats {
  private final LongAdderRateGauge pollRequestSensor;
  private final Lazy<Sensor> pollRequestLatencySensor;
  private final Sensor pollResultNumSensor;
  private final LongAdderRateGauge pollNonZeroResultNumSensor;

  private final Lazy<Sensor> pollRequestError;
  private final Lazy<Sensor> consumerRecordsProducingToWriterBufferLatencySensor;
  private final Lazy<Sensor> detectedDeletedTopicNumSensor;
  private final Lazy<Sensor> detectedNoRunningIngestionTopicPartitionNumSensor;
  private final Lazy<Sensor> delegateSubscribeLatencySensor;
  private final Lazy<Sensor> updateCurrentAssignmentLatencySensor;
  private final Lazy<Sensor> maxPartitionsPerConsumer;
  private final Lazy<Sensor> minPartitionsPerConsumer;
  private final Lazy<Sensor> avgPartitionsPerConsumer;
  private final Lazy<Sensor> getOffsetLagIsAbsentSensor;
  private final Lazy<Sensor> getOffsetLagIsPresentSensor;
  private final Lazy<Sensor> getLatestOffsetIsAbsentSensor;
  private final Lazy<Sensor> getLatestOffsetIsPresentSensor;
  private final Sensor byteSizeSensor;
  private final Lazy<Sensor> idleTimeSensor;

  public KafkaConsumerServiceStats(
      MetricsRepository metricsRepository,
      String storeName,
      LongSupplier getMaxElapsedTimeSinceLastPollInConsumerPool,
      KafkaConsumerServiceStats totalStats,
      Time time) {
    super(metricsRepository, storeName);
    /**
     * Below are the sensors that are recording total stats per region and per-store stats.
     */

    // The bytes of polled pubsub messages for each poll request
    byteSizeSensor =
        registerPerStoreAndTotalSensor("bytes_per_poll", totalStats, () -> totalStats.byteSizeSensor, minAndMax());
    // the number of messages returned by Kafka consumer poll.
    pollResultNumSensor = registerPerStoreAndTotalSensor(
        "consumer_poll_result_num",
        totalStats,
        () -> totalStats.pollResultNumSensor,
        new Avg(),
        new Min());

    /**
     * Below are the sensors that are recording total stats per each region
     */

    // the consumer idle time
    idleTimeSensor = Lazy.of(() -> registerSensor("idle_time", new Max()));
    // the number of poll requests
    pollRequestSensor =
        registerOnlyTotalRate("consumer_poll_request", totalStats, () -> totalStats.pollRequestSensor, time);
    // Notice that "pollRequestLatencySensor" only reports correct data when consumer task threads are not stuck
    pollRequestLatencySensor = Lazy.of(() -> registerSensor("consumer_poll_request_latency", new Avg(), new Max()));
    pollNonZeroResultNumSensor = registerOnlyTotalRate(
        "consumer_poll_non_zero_result_num",
        totalStats,
        () -> totalStats.pollNonZeroResultNumSensor,
        time);
    /**
     * "max_elapsed_time_since_last_successful_poll" is a Gauge metric which calls a function inside KafkaConsumerService,
     *  this metric will still be reported per minute with the latest result from the function even if consumer task
     *  threads are stuck. No need to keep a class property for it since we never call record on it.
     */
    registerSensor(
        new AsyncGauge(
            (ignored, ignored2) -> getMaxElapsedTimeSinceLastPollInConsumerPool.getAsLong(),
            "max_elapsed_time_since_last_successful_poll"));
    // consumer record number per second returned by Kafka consumer poll.

    pollRequestError = Lazy.of(() -> registerSensor("consumer_poll_error", new OccurrenceRate()));
    // To measure 'put' latency of consumer records blocking queue
    consumerRecordsProducingToWriterBufferLatencySensor =
        Lazy.of(() -> registerSensor("consumer_records_producing_to_write_buffer_latency", new Avg(), new Max()));
    detectedDeletedTopicNumSensor = Lazy.of(() -> registerSensor("detected_deleted_topic_num", new Total()));
    detectedNoRunningIngestionTopicPartitionNumSensor =
        Lazy.of(() -> registerSensor("detected_no_running_ingestion_topic_partition_num", new Total()));
    delegateSubscribeLatencySensor = Lazy.of(() -> registerSensor("delegate_subscribe_latency", new Avg(), new Max()));
    updateCurrentAssignmentLatencySensor =
        Lazy.of(() -> registerSensor("update_current_assignment_latency", new Avg(), new Max()));

    minPartitionsPerConsumer = Lazy.of(() -> registerSensor("min_partitions_per_consumer", new Gauge()));
    maxPartitionsPerConsumer = Lazy.of(() -> registerSensor("max_partitions_per_consumer", new Gauge()));
    avgPartitionsPerConsumer = Lazy.of(() -> registerSensor("avg_partitions_per_consumer", new Gauge()));

    Lazy<Sensor[]> offsetLagParent =
        Lazy.of(() -> new Sensor[] { registerSensor("getOffsetLag", new OccurrenceRate()) });
    this.getOffsetLagIsAbsentSensor =
        Lazy.of(() -> registerSensor("getOffsetLagIsAbsent", offsetLagParent.get(), new OccurrenceRate()));
    this.getOffsetLagIsPresentSensor =
        Lazy.of(() -> registerSensor("getOffsetLagIsPresent", offsetLagParent.get(), new OccurrenceRate()));

    Lazy<Sensor[]> latestOffsetParent =
        Lazy.of(() -> new Sensor[] { registerSensor("getLatestOffset", new OccurrenceRate()) });
    this.getLatestOffsetIsAbsentSensor =
        Lazy.of(() -> registerSensor("getLatestOffsetIsAbsent", latestOffsetParent.get(), new OccurrenceRate()));
    this.getLatestOffsetIsPresentSensor =
        Lazy.of(() -> registerSensor("getLatestOffsetIsPresent", latestOffsetParent.get(), new OccurrenceRate()));
  }

  public void recordPollRequestLatency(double latency) {
    pollRequestSensor.record();
    pollRequestLatencySensor.get().record(latency);
  }

  public void recordPollResultNum(int count) {
    pollResultNumSensor.record(count);
  }

  public void recordNonZeroPollResultNum(int count) {
    pollNonZeroResultNumSensor.record(count);
  }

  public void recordConsumerRecordsProducingToWriterBufferLatency(double latency) {
    consumerRecordsProducingToWriterBufferLatencySensor.get().record(latency);
  }

  public void recordPollError() {
    pollRequestError.get().record();
  }

  public void recordDetectedDeletedTopicNum(int count) {
    detectedDeletedTopicNumSensor.get().record(count);
  }

  public void recordDetectedNoRunningIngestionTopicPartitionNum(int count) {
    detectedNoRunningIngestionTopicPartitionNumSensor.get().record(count);
  }

  public void recordDelegateSubscribeLatency(double value) {
    delegateSubscribeLatencySensor.get().record(value);
  }

  public void recordUpdateCurrentAssignmentLatency(double value) {
    updateCurrentAssignmentLatencySensor.get().record(value);
  }

  public void recordMinPartitionsPerConsumer(int count) {
    minPartitionsPerConsumer.get().record(count);
  }

  public void recordMaxPartitionsPerConsumer(int count) {
    maxPartitionsPerConsumer.get().record(count);
  }

  public void recordAvgPartitionsPerConsumer(int count) {
    avgPartitionsPerConsumer.get().record(count);
  }

  public void recordOffsetLagIsAbsent() {
    getOffsetLagIsAbsentSensor.get().record();
  }

  public void recordOffsetLagIsPresent() {
    getOffsetLagIsPresentSensor.get().record();
  }

  public void recordLatestOffsetIsAbsent() {
    getLatestOffsetIsAbsentSensor.get().record();
  }

  public void recordLatestOffsetIsPresent() {
    getLatestOffsetIsPresentSensor.get().record();
  }

  public void recordByteSizePerPoll(double count) {
    byteSizeSensor.record(count);
  }

  public void recordConsumerIdleTime(double time) {
    idleTimeSensor.get().record(time);
  }
}
