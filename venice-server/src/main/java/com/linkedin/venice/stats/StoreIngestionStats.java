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

import static com.linkedin.venice.stats.StatsErrorCode.*;

public class StoreIngestionStats extends AbstractVeniceStats{
  private StoreIngestionTask storeIngestionTask;

  // Bytes processed by the store ingestion task as a rate
  private final Sensor bytesConsumedSensor;
  /*
   * Bytes read from Kafka by store ingestion task as a total. This metric includes bytes read for all store versions
   * allocated in a storage node reported with its uncompressed data size.
   */
  private final Sensor bytesReadFromKafkaAsUncompressedSizeSensor;
  private final Sensor recordsConsumedSensor;
  private final Sensor storageQuotaUsedSensor;
  // disk quota allowed for a store without replication. It should be as a straight line unless we bumps the disk quota allowed.
  private final Sensor diskQuotaSensor;

  private final Sensor pollRequestSensor;
  private final Sensor pollRequestLatencySensor;
  private final Sensor pollResultNumSensor;
  private final Sensor consumerRecordsQueuePutLatencySensor;
  private final Sensor keySizeSensor;
  private final Sensor valueSizeSensor;

  private final Sensor unexpectedMessageSensor;
  private final Sensor inconsistentStoreMetadataSensor;

  private final Sensor ingestionFailureSensor;

  /**
   * A gauge reporting the total the percentage of hybrid quota used.
   */
  private double hybridQuotaUsageGauge;

  /**
   * A gauge reporting the disk capacity allowed for a store
   */
  private long diskQuotaAllowedGauge;

  // Measure the avg/max time we need to spend on waiting for the leader producer
  private final Sensor leaderProducerSynchronizeLatencySensor;
  // Measure the avg/max latency for data lookup and deserialization
  private final Sensor leaderWriteComputeLookUpLatencySensor;
  // Measure the avg/max latency for the actual write computation
  private final Sensor leaderWriteComputeUpdateLatencySensor;

  // Measure the latency in processing consumer actions
  private final Sensor processConsumerActionLatencySensor;
  // Measure the latency in checking long running task states, like leader promotion, TopicSwitch
  private final Sensor checkLongRunningTasksLatencySensor;
  // Measure the latency in enforcing hybrid store disk quota
  private final Sensor quotaEnforcementLatencySensor;
  // Measure the latency from "after polling records from Kafka" to "successfully put records in to drainer queue"
  private final Sensor consumerToQueueLatencySensor;
  // Measure the latency in putting data into storage engine
  private final Sensor storageEnginePutLatencySensor;

  /**
   * Measure the call count of {@literal StoreIngestionTask#produceToStoreBufferService}.
   *
   */
  private final Sensor produceToDrainerQueueCallCountSensor;
  /**
   * Measure the record number passed to {@literal StoreIngestionTask#produceToStoreBufferService}.
   */
  private final Sensor produceToDrainerQueueRecordNumSensor;

  public StoreIngestionStats(MetricsRepository metricsRepository,
                             String storeName) {
    super(metricsRepository, storeName);
    this.storeIngestionTask = null;

    bytesConsumedSensor = registerSensor("bytes_consumed", new Rate());
    bytesReadFromKafkaAsUncompressedSizeSensor = registerSensor("bytes_read_from_kafka_as_uncompressed_size", new Rate(), new Total());
    recordsConsumedSensor = registerSensor("records_consumed", new Rate());
    diskQuotaSensor = registerSensor("global_store_disk_quota_allowed",
                                      new Gauge(() -> diskQuotaAllowedGauge), new Max());

    // Measure latency of Kafka consumer poll request and processing returned consumer records
    pollRequestSensor = registerSensor("kafka_poll_request", new Count());
    pollRequestLatencySensor = registerSensor("kafka_poll_request_latency", new Avg(), new Max());
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

    unexpectedMessageSensor = registerSensor("unexpected_message", new Rate());
    inconsistentStoreMetadataSensor = registerSensor("inconsistent_store_metadata", new Count());


    ingestionFailureSensor = registerSensor("ingestion_failure", new Count());

    storageQuotaUsedSensor = registerSensor("storage_quota_used",
                                            new Gauge(() -> hybridQuotaUsageGauge), new Avg(), new Min(), new Max());

    leaderProducerSynchronizeLatencySensor = registerSensor("leader_producer_synchronize_latency", new Avg(), new Max());
    leaderWriteComputeLookUpLatencySensor = registerSensor("leader_write_compute_lookup_latency", new Avg(), new Max());
    leaderWriteComputeUpdateLatencySensor = registerSensor("leader_write_compute_update_latency", new Avg(), new Max());

    processConsumerActionLatencySensor = registerSensor("process_consumer_actions_latency", new Avg(), new Max());
    checkLongRunningTasksLatencySensor = registerSensor("check_long_running_task_latency", new Avg(), new Max());
    quotaEnforcementLatencySensor = registerSensor("hybrid_quota_enforcement_latency", new Avg(), new Max());
    consumerToQueueLatencySensor = registerSensor("consumer_to_queue_latency", new Avg(), new Max());
    storageEnginePutLatencySensor = registerSensor("storage_engine_put_latency", new Avg(), new Max());
    produceToDrainerQueueCallCountSensor = registerSensor("produce_to_drainer_queue_call_count", new Rate());
    produceToDrainerQueueRecordNumSensor = registerSensor("produce_to_drainer_queue_record_num", new Avg(), new Max());
  }

  public StoreIngestionTask getStoreIngestionTask() {
    return storeIngestionTask;
  }

  public void recordBytesConsumed(long bytes) {
    bytesConsumedSensor.record(bytes);
  }

  public void recordBytesReadFromKafkaAsUncompressedSize(long bytes) {
    bytesReadFromKafkaAsUncompressedSizeSensor.record(bytes);
  }

  public void recordRecordsConsumed(int count) {
    recordsConsumedSensor.record(count);
  }

  public void recordStorageQuotaUsed(double quotaUsed) {
    hybridQuotaUsageGauge = quotaUsed;
    storageQuotaUsedSensor.record(quotaUsed);
  }

  public void recordDiskQuotaAllowed(long quotaAllowed) {
    diskQuotaAllowedGauge = quotaAllowed;
    diskQuotaSensor.record(quotaAllowed);
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

  public void recordUnexpectedMessage(int count) {
    unexpectedMessageSensor.record(count);
  }

  public void recordInconsistentStoreMetadata(int count) { inconsistentStoreMetadataSensor.record(count); }

  public void recordKeySize(long bytes) {
    keySizeSensor.record(bytes);
  }

  public void recordValueSize(long bytes) {
    valueSizeSensor.record(bytes);
  }

  public void recordIngestionFailure() {
    ingestionFailureSensor.record();
  }

  public void recordLeaderProducerSynchronizeLatency(double latency) {
    leaderProducerSynchronizeLatencySensor.record(latency);
  }

  public void recordWriteComputeLookUpLatency(double latency) {
    leaderWriteComputeLookUpLatencySensor.record(latency);
  }

  public void recordWriteComputeUpdateLatency(double latency) {
    leaderWriteComputeUpdateLatencySensor.record(latency);
  }

  public void recordProcessConsumerActionLatency(double latency) {
    processConsumerActionLatencySensor.record(latency);
  }

  public void recordCheckLongRunningTasksLatency(double latency) {
    checkLongRunningTasksLatencySensor.record(latency);
  }

  public void recordQuotaEnforcementLatency(double latency) {
    quotaEnforcementLatencySensor.record(latency);
  }

  public void recordConsumerToQueueLatency(double latency) {
    consumerToQueueLatencySensor.record(latency);
  }

  public void recordStorageEnginePutLatency(double latency) {
    storageEnginePutLatencySensor.record(latency);
  }

  public void recordProduceToDrainQueueRecordNum(int recordNum) {
    produceToDrainerQueueCallCountSensor.record();
    produceToDrainerQueueRecordNumSensor.record(recordNum);
  }

  private static class StoreIngestionStatsCounter extends LambdaStat {
    StoreIngestionStatsCounter(StoreIngestionStats stats, Supplier<Long> supplier) {
      super(() -> {
        StoreIngestionTask task = stats.getStoreIngestionTask();
        if (task != null && task.isRunning()) {
          return (double) supplier.get();
        } else {
          return INACTIVE_STORE_INGESTION_TASK.code;
        }
      });
    }
  }
}
