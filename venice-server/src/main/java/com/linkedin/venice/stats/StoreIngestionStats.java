package com.linkedin.venice.stats;

import com.linkedin.venice.kafka.consumer.StoreIngestionTask;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Count;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.Min;
import io.tehuti.metrics.stats.Rate;
import io.tehuti.metrics.stats.Total;
import java.util.function.Supplier;


public class StoreIngestionStats extends AbstractVeniceStats{
  private StoreIngestionTask storeIngestionTask;

  private final Sensor bytesConsumedSensor;
  private final Sensor recordsConsumedSensor;

  private final Sensor pollRequestSensor;
  private final Sensor pollRequestLatencySensor;
  private final Sensor pollResultNumSensor;
  private final Sensor consumerRecordsQueuePutLatencySensor;
  private final Sensor keySizeSensor;
  private final Sensor valueSizeSensor;


  public StoreIngestionStats(MetricsRepository metricsRepository,
                             String storeName) {
    super(metricsRepository, storeName);
    this.storeIngestionTask = null;

    bytesConsumedSensor = registerSensor("bytes_consumed", new Rate());
    recordsConsumedSensor = registerSensor("records_consumed", new Rate());

    //KafkaOffsetLag is the lag of the most recent offsets between local kafka and storage node.
    //If a storage node has multiple partitions for a store, they will be aggregated.
    //Agg store doesn't have offset lag
    registerSensor("kafka_offset_lag", new StoreIngestionStatsCounter(this, () -> storeIngestionTask.getOffsetLag()));

    // Measure latency of Kafka consumer poll request and processing returned consumer records
    pollRequestSensor = registerSensor("kafka_poll_request", new Count());
    pollRequestLatencySensor = registerSensor("kafka_poll_request_latency", new Avg());
    // consumer record number per second returned by Kafka consumer poll.
    pollResultNumSensor = registerSensor("kafka_poll_result_num", new Avg(), new Total());
    // To measure 'put' latency of consumer records blocking queue
    consumerRecordsQueuePutLatencySensor = registerSensor("consumer_records_queue_put_latency", new Avg(), new Max());

    String keySizeSensorName = "record_key_size_in_bytes";
    keySizeSensor = registerSensor(keySizeSensorName, new Avg(), new Min(), new Max(),
        TehutiUtils.getPercentileStat(getName() + AbstractVeniceStats.DELIMITER + keySizeSensorName, 40000, 1000000));

    String valueSizeSensorName = "record_value_size_in_bytes";
    valueSizeSensor = registerSensor(valueSizeSensorName, new Avg(), new Min(), new Max(),
        TehutiUtils.getPercentileStat(getName() + AbstractVeniceStats.DELIMITER + valueSizeSensorName, 40000, 1000000));
  }

  public void updateStoreConsumptionTask(StoreIngestionTask task) {
    storeIngestionTask = task;

    //TODO: It would be much better to apply versioned stats pattern for these metrics.
    if (task.isHybridMode()) {
      registerSensor("largest_version_kafka_real_time_buffer_offset_lag", new StoreIngestionStatsCounter(this,
          () -> storeIngestionTask.getRealTimeBufferOffsetLag()));
      registerSensor("largest_version_number_of_partitions_not_receive_SOBR", new StoreIngestionStatsCounter(this,
          () -> storeIngestionTask.getNumOfPartitionsNotReceiveSOBR()));
    }
  }

  public StoreIngestionTask getStoreIngestionTask() {
    return storeIngestionTask;
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

  public void recordPollResultNum(int count) {
    pollResultNumSensor.record(count);
  }

  public void recordConsumerRecordsQueuePutLatency(double latency) {
    consumerRecordsQueuePutLatencySensor.record(latency);
  }

  public void recordKeySize(long bytes) {
    keySizeSensor.record(bytes);
  }

  public void recordValueSize(long bytes) {
    valueSizeSensor.record(bytes);
  }

  private static class StoreIngestionStatsCounter extends LambdaStat {
    StoreIngestionStatsCounter(StoreIngestionStats stats, Supplier<Long> supplier) {
      super(() -> {
        StoreIngestionTask task = stats.getStoreIngestionTask();
        if (task != null && task.isRunning()) {
          return (double) supplier.get();
        } else {
          return 0d;
        }
      });
    }
  }
}
