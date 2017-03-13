package com.linkedin.venice.stats;

import com.linkedin.venice.kafka.consumer.StoreConsumptionTask;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Count;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.Rate;
import io.tehuti.metrics.stats.Total;

public class StoreConsumptionStats extends AbstractVeniceStats{
  private StoreConsumptionTask storeConsumptionTask;

  private final Sensor bytesConsumedSensor;
  private final Sensor recordsConsumedSensor;

  private final Sensor kafkaOffsetLagSensor;

  private final Sensor pollRequestSensor;
  private final Sensor pollRequestLatencySensor;
  private final Sensor processPollResultLatencySensor;
  private final Sensor pollResultNumSensor;
  private final Sensor consumerRecordsQueuePutLatencySensor;
  private final Sensor consumerRecordsQueuePollLatencySensor;


  public StoreConsumptionStats(MetricsRepository metricsRepository,
                               String storeName) {
    super(metricsRepository, storeName);
    this.storeConsumptionTask = null;

    bytesConsumedSensor = registerSensor("bytes_consumed", new Rate());
    recordsConsumedSensor = registerSensor("records_consumed", new Rate());

    //KafkaOffsetLag is the lag of the most recent offsets between local kafka and storage node.
    //If a storage node has multiple partitions for a store, they will be aggregated.
    //Agg store doesn't have offset lag
    kafkaOffsetLagSensor = registerSensor("kafka_offset_lag", new OffsetLagStat(this));

    // Measure latency of Kafka consumer poll request and processing returned consumer records
    pollRequestSensor = registerSensor("kafka_poll_request", new Count());
    pollRequestLatencySensor = registerSensor("kafka_poll_request_latency", new Avg());
    processPollResultLatencySensor = registerSensor("process_poll_result_latency", new Avg());
    // consumer record number per second returned by Kafka consumer poll.
    pollResultNumSensor = registerSensor("kafka_poll_result_num", new Avg(), new Total());
    // To measure 'put' latency of consumer records blocking queue
    consumerRecordsQueuePutLatencySensor = registerSensor("consumer_records_queue_put_latency", new Avg(), new Max());
    consumerRecordsQueuePollLatencySensor = registerSensor("consumer_records_queue_poll_latency", new Avg(), new Max());
  }

  public void updateStoreConsumptionTask(StoreConsumptionTask task) {
    storeConsumptionTask = task;
  }

  public StoreConsumptionTask getStoreConsumptionTask() {
    return storeConsumptionTask;
  }

  public void recordBytesConsumed(long bytes) {
    bytesConsumedSensor.record(bytes);
  }

  public void recordRecordsConsumed(int count) {
    recordsConsumedSensor.record(count);
  }

  public void recordPollRequestLatency(double latency) {
    pollRequestSensor.record();
    pollRequestLatencySensor.record(latency);
  }

  public void recordProcessPollResultLatency(double latency) {
    processPollResultLatencySensor.record(latency);
  }

  public void recordPollResultNum(int count) {
    pollResultNumSensor.record(count);
  }

  public void recordConsumerRecordsQueuePutLatency(double latency) {
    consumerRecordsQueuePutLatencySensor.record(latency);
  }

  public void recordConsumerRecordsQueuePollLatency(double latency) {
    consumerRecordsQueuePollLatencySensor.record(latency);
  }

  private static class OffsetLagStat extends LambdaStat {
    public OffsetLagStat(StoreConsumptionStats stats) {
      super(() -> {
        StoreConsumptionTask task = stats.getStoreConsumptionTask();
        if (task != null && task.isRunning()) {
          double a = task.getOffsetLag();
          return a;
        }
        else {
          return 0d;
        }
      });
    }
  }
}
