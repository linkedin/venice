package com.linkedin.davinci.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.OccurrenceRate;
import io.tehuti.metrics.stats.Total;


public class KafkaConsumerServiceStats extends AbstractVeniceStats {
  private final Sensor pollRequestSensor;
  private final Sensor pollRequestLatencySensor;
  private final Sensor pollResultNumSensor;
  private final Sensor pollRequestError;
  private final Sensor consumerRecordsProducingToWriterBufferLatencySensor;
  private final Sensor detectedDeletedTopicNumSensor;
  private final Sensor detectedNoRunningIngestionTopicNumSensor;

  public KafkaConsumerServiceStats(MetricsRepository metricsRepository) {
    super(metricsRepository, "kafka_consumer_service");

    pollRequestSensor = registerSensor("consumer_poll_request", new OccurrenceRate());
    pollRequestLatencySensor = registerSensor("consumer_poll_request_latency", new Avg(), new Max());
    // consumer record number per second returned by Kafka consumer poll.
    pollResultNumSensor = registerSensor("consumer_poll_result_num", new Avg(), new Total());
    pollRequestError = registerSensor("consumer_poll_error", new OccurrenceRate());
    // To measure 'put' latency of consumer records blocking queue
    consumerRecordsProducingToWriterBufferLatencySensor = registerSensor("consumer_records_producing_to_write_buffer_latency", new Avg(), new Max());
    detectedDeletedTopicNumSensor = registerSensor("detected_deleted_topic_num", new Total());
    detectedNoRunningIngestionTopicNumSensor = registerSensor("detected_no_running_ingestion_topic_num", new Total());
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

  public void recordDetectedNoRunningIngestionTopicNum(int count) {
    detectedNoRunningIngestionTopicNumSensor.record(count);
  }
}
