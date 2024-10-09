package com.linkedin.davinci.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.LongAdderRateGauge;
import com.linkedin.venice.utils.Time;
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
  private final Sensor pollRequestLatencySensor;
  private final Sensor pollResultNumSensor;
  private final LongAdderRateGauge pollNonZeroResultNumSensor;

  private final Sensor pollRequestError;
  private final Sensor consumerRecordsProducingToWriterBufferLatencySensor;
  private final Sensor detectedDeletedTopicNumSensor;
  private final Sensor detectedNoRunningIngestionTopicPartitionNumSensor;
  private final Sensor delegateSubscribeLatencySensor;
  private final Sensor updateCurrentAssignmentLatencySensor;
  private final Sensor maxPartitionsPerConsumer;
  private final Sensor minPartitionsPerConsumer;
  private final Sensor avgPartitionsPerConsumer;
  private final Sensor subscribedPartitionsNum;
  private final Sensor getOffsetLagIsAbsentSensor;
  private final Sensor getOffsetLagIsPresentSensor;
  private final Sensor getLatestOffsetIsAbsentSensor;
  private final Sensor getLatestOffsetIsPresentSensor;
  private final Sensor byteSizeSensor;
  private final Sensor idleTimeSensor;

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
    idleTimeSensor = registerSensor("idle_time", new Max());
    // the number of poll requests
    pollRequestSensor =
        registerOnlyTotalRate("consumer_poll_request", totalStats, () -> totalStats.pollRequestSensor, time);
    // Notice that "pollRequestLatencySensor" only reports correct data when consumer task threads are not stuck
    pollRequestLatencySensor = registerSensor("consumer_poll_request_latency", new Avg(), new Max());
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

    pollRequestError = registerSensor("consumer_poll_error", new OccurrenceRate());
    // To measure 'put' latency of consumer records blocking queue
    consumerRecordsProducingToWriterBufferLatencySensor =
        registerSensor("consumer_records_producing_to_write_buffer_latency", new Avg(), new Max());
    detectedDeletedTopicNumSensor = registerSensor("detected_deleted_topic_num", new Total());
    detectedNoRunningIngestionTopicPartitionNumSensor =
        registerSensor("detected_no_running_ingestion_topic_partition_num", new Total());
    delegateSubscribeLatencySensor = registerSensor("delegate_subscribe_latency", new Avg(), new Max());
    updateCurrentAssignmentLatencySensor = registerSensor("update_current_assignment_latency", new Avg(), new Max());

    minPartitionsPerConsumer = registerSensor("min_partitions_per_consumer", new Gauge());
    maxPartitionsPerConsumer = registerSensor("max_partitions_per_consumer", new Gauge());
    avgPartitionsPerConsumer = registerSensor("avg_partitions_per_consumer", new Gauge());
    subscribedPartitionsNum = registerSensor("subscribed_partitions_num", new Gauge());

    Sensor getOffsetLagSensor = registerSensor("getOffsetLag", new OccurrenceRate());
    Sensor[] offsetLagParent = new Sensor[] { getOffsetLagSensor };
    this.getOffsetLagIsAbsentSensor = registerSensor("getOffsetLagIsAbsent", offsetLagParent, new OccurrenceRate());
    this.getOffsetLagIsPresentSensor = registerSensor("getOffsetLagIsPresent", offsetLagParent, new OccurrenceRate());

    Sensor getLatestOffsetSensor = registerSensor("getLatestOffset", new OccurrenceRate());
    Sensor[] latestOffsetParent = new Sensor[] { getLatestOffsetSensor };
    this.getLatestOffsetIsAbsentSensor =
        registerSensor("getLatestOffsetIsAbsent", latestOffsetParent, new OccurrenceRate());
    this.getLatestOffsetIsPresentSensor =
        registerSensor("getLatestOffsetIsPresent", latestOffsetParent, new OccurrenceRate());
  }

  public void recordPollRequestLatency(double latency) {
    pollRequestSensor.record();
    pollRequestLatencySensor.record(latency);
  }

  public void recordPollResultNum(int count) {
    pollResultNumSensor.record(count);
  }

  public void recordNonZeroPollResultNum(int count) {
    pollNonZeroResultNumSensor.record(count);
  }

  public void recordConsumerRecordsProducingToWriterBufferLatency(double latency) {
    consumerRecordsProducingToWriterBufferLatencySensor.record(latency);
  }

  public void recordPollError() {
    pollRequestError.record();
  }

  public void recordDetectedDeletedTopicNum(int count) {
    detectedDeletedTopicNumSensor.record(count);
  }

  public void recordDetectedNoRunningIngestionTopicPartitionNum(int count) {
    detectedNoRunningIngestionTopicPartitionNumSensor.record(count);
  }

  public void recordDelegateSubscribeLatency(double value) {
    delegateSubscribeLatencySensor.record(value);
  }

  public void recordUpdateCurrentAssignmentLatency(double value) {
    updateCurrentAssignmentLatencySensor.record(value);
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

  public void recordOffsetLagIsAbsent() {
    getOffsetLagIsAbsentSensor.record();
  }

  public void recordOffsetLagIsPresent() {
    getOffsetLagIsPresentSensor.record();
  }

  public void recordLatestOffsetIsAbsent() {
    getLatestOffsetIsAbsentSensor.record();
  }

  public void recordLatestOffsetIsPresent() {
    getLatestOffsetIsPresentSensor.record();
  }

  public void recordByteSizePerPoll(double count) {
    byteSizeSensor.record(count);
  }

  public void recordConsumerIdleTime(double time) {
    idleTimeSensor.record(time);
  }

  public void recordSubscribedPartitionsNum(int count) {
    subscribedPartitionsNum.record(count);
  }
}
