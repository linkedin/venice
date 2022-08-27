package com.linkedin.davinci.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.Gauge;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Count;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.OccurrenceRate;
import io.tehuti.metrics.stats.Total;
import java.util.function.LongSupplier;


public class KafkaConsumerServiceStats extends AbstractVeniceStats {
  private final Sensor pollRequestSensor;
  private final Sensor pollRequestLatencySensor;
  private final Sensor pollResultNumSensor;
  private final Sensor pollRequestError;
  private final Sensor consumerRecordsProducingToWriterBufferLatencySensor;
  private final Sensor detectedDeletedTopicNumSensor;
  private final Sensor detectedNoRunningIngestionTopicPartitionNumSensor;
  private final Sensor delegateSubscribeLatencySensor;
  private final Sensor updateCurrentAssignmentLatencySensor;
  private final Sensor consumerSelectionForTopicError;
  private final Sensor maxPartitionsPerConsumer;
  private final Sensor minPartitionsPerConsumer;
  private final Sensor avgPartitionsPerConsumer;
  private final Sensor getOffsetLagIsAbsentSensor;
  private final Sensor getOffsetLagIsPresentSensor;
  private final Sensor getLatestOffsetIsAbsentSensor;
  private final Sensor getLatestOffsetIsPresentSensor;

  public KafkaConsumerServiceStats(
      MetricsRepository metricsRepository,
      String nameWithKafkaClusterAlias,
      LongSupplier getMaxElapsedTimeSinceLastPollInConsumerPool) {
    super(metricsRepository, nameWithKafkaClusterAlias);

    pollRequestSensor = registerSensor("consumer_poll_request", new OccurrenceRate());
    // Notice that "pollRequestLatencySensor" only reports correct data when consumer task threads are not stuck
    pollRequestLatencySensor = registerSensor("consumer_poll_request_latency", new Avg(), new Max());
    /**
     * "max_elapsed_time_since_last_successful_poll" is a Gauge metric which calls a function inside KafkaConsumerService,
     *  this metric will still be reported per minute with the latest result from the function even if consumer task
     *  threads are stuck. No need to keep a class property for it since we never call record on it.
     */
    registerSensor(
        "max_elapsed_time_since_last_successful_poll",
        new Gauge(getMaxElapsedTimeSinceLastPollInConsumerPool.getAsLong()));
    // consumer record number per second returned by Kafka consumer poll.
    pollResultNumSensor = registerSensor("consumer_poll_result_num", new Avg(), new Total());
    pollRequestError = registerSensor("consumer_poll_error", new OccurrenceRate());
    // To measure 'put' latency of consumer records blocking queue
    consumerRecordsProducingToWriterBufferLatencySensor =
        registerSensor("consumer_records_producing_to_write_buffer_latency", new Avg(), new Max());
    detectedDeletedTopicNumSensor = registerSensor("detected_deleted_topic_num", new Total());
    detectedNoRunningIngestionTopicPartitionNumSensor =
        registerSensor("detected_no_running_ingestion_topic_partition_num", new Total());
    delegateSubscribeLatencySensor = registerSensor("delegate_subscribe_latency", new Avg(), new Max());
    updateCurrentAssignmentLatencySensor = registerSensor("update_current_assignment_latency", new Avg(), new Max());

    // To monitor cases when a shared consumer cannot be selected
    consumerSelectionForTopicError = registerSensor("consumer_selection_for_topic_error", new Count());

    minPartitionsPerConsumer = registerSensor("min_partitions_per_consumer", new Gauge());
    maxPartitionsPerConsumer = registerSensor("max_partitions_per_consumer", new Gauge());
    avgPartitionsPerConsumer = registerSensor("avg_partitions_per_consumer", new Gauge());

    Sensor getOffsetLagSensor = registerSensor("getOffsetLag", new OccurrenceRate());
    Sensor[] offsetLagParent = new Sensor[] { getOffsetLagSensor };
    this.getOffsetLagIsAbsentSensor =
        registerSensor("getOffsetLagIsAbsent", null, offsetLagParent, new OccurrenceRate());
    this.getOffsetLagIsPresentSensor =
        registerSensor("getOffsetLagIsPresent", null, offsetLagParent, new OccurrenceRate());

    Sensor getLatestOffsetSensor = registerSensor("getLatestOffset", new OccurrenceRate());
    Sensor[] latestOffsetParent = new Sensor[] { getLatestOffsetSensor };
    this.getLatestOffsetIsAbsentSensor =
        registerSensor("getLatestOffsetIsAbsent", null, latestOffsetParent, new OccurrenceRate());
    this.getLatestOffsetIsPresentSensor =
        registerSensor("getLatestOffsetIsPresent", null, latestOffsetParent, new OccurrenceRate());

  }

  public void recordPollRequestLatency(double latency) {
    pollRequestSensor.record();
    pollRequestLatencySensor.record(latency);
  }

  public void recordPollResultNum(int count) {
    pollResultNumSensor.record(count);
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

  public void recordConsumerSelectionForTopicError() {
    consumerSelectionForTopicError.record();
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
}
