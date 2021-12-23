package com.linkedin.davinci.stats;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.kafka.consumer.PartitionConsumptionState;
import com.linkedin.davinci.kafka.consumer.StoreIngestionTask;
import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.Gauge;
import com.linkedin.venice.stats.LambdaStat;
import com.linkedin.venice.stats.TehutiUtils;
import com.linkedin.venice.utils.RegionUtils;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Count;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.Min;
import io.tehuti.metrics.stats.Rate;
import io.tehuti.metrics.stats.Total;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import static com.linkedin.venice.stats.StatsErrorCode.*;

public class StoreIngestionStats extends AbstractVeniceStats {
  /**
   * DO NOT REMOVE the aggregated consumption rate metrics
   * It's needed for Venice-health dashboard.
   */
  // The aggregated bytes ingested rate for the entire host
  private final Sensor totalBytesConsumedSensor;
  // The aggregated records ingested rate for the entire host
  private final Sensor totalRecordsConsumedSensor;

  /*
   * Bytes read from Kafka by store ingestion task as a total. This metric includes bytes read for all store versions
   * allocated in a storage node reported with its uncompressed data size.
   */
  private final Sensor bytesReadFromKafkaAsUncompressedSizeSensor;
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
   * Sensors for emitting if/when we detect DCR violations (such as a backwards timestamp or receding offset vector)
   */
  private final Sensor timestampRegresssionDCRErrorRate;
  private final Sensor offsetRegressionDCRErrorRate;

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
   * Measure the call count of {@literal StoreIngestionTask#produceToStoreBufferServiceOrKafka}.
   *
   */
  private final Sensor produceToDrainerQueueCallCountSensor;
  /**
   * Measure the record number passed to {@literal StoreIngestionTask#produceToStoreBufferServiceOrKafka}.
   */
  private final Sensor produceToDrainerQueueRecordNumSensor;

  /**
   * Measure the number of record produced to kafka {@literal StoreIngestionTask#produceToStoreBufferServiceOrKafka}.
   */
  private final Sensor produceToKafkaRecordNumSensor;

  /**
   * Measure the latency of producing record to kafka {@literal StoreIngestionTask#produceToStoreBufferServiceOrKafka}.
   */
  private final Sensor produceToKafkaLatencySensor;

  /**
   * Measure the number of times a record was found in {@link PartitionConsumptionState#transientRecordMap} during UPDATE
   * message processing.
   */
  private final Sensor writeComputeCacheHitCount;


  private final Sensor totalLeaderBytesConsumedSensor;
  private final Sensor totalLeaderRecordsConsumedSensor;
  private final Sensor totalFollowerBytesConsumedSensor;
  private final Sensor totalFollowerRecordsConsumedSensor;
  private final Sensor totalLeaderBytesProducedSensor;
  private final Sensor totalLeaderRecordsProducedSensor;
  private final Map<Integer, Sensor> totalRegionHybridBytesConsumedMap;
  private final Map<Integer, Sensor> totalRegionHybridRecordsConsumedMap;

  private final Sensor checksumVerificationFailureSensor;

  /**
   * Measure the number of times replication metadata was found in {@link PartitionConsumptionState#transientRecordMap}
   */
  private final Sensor leaderIngestionReplicationMetadataCacheHitCount;

  /**
   * Measure the avg/max latency for value bytes lookup
   */
  private final Sensor leaderIngestionValueBytesLookUpLatencySensor;

  /**
   * Measure the number of times value bytes were found in {@link PartitionConsumptionState#transientRecordMap}
   */
  private final Sensor leaderIngestionValueBytesCacheHitCount;

  /**
   * Measure the avg/max latency for replication metadata data lookup
   */
  private final Sensor leaderIngestionReplicationMetadataLookUpLatencySensor;

  /**
   * Measure the count of ignored updates due to conflict resolution
   */
  private final Sensor updateIgnoredDCRSensor;

  /**
   * Measure the count of tombstones created
   */
  private final Sensor tombstoneCreationDCRSensor;

  private final Sensor leaderDelegateRealTimeRecordLatencySensor;

  public StoreIngestionStats(MetricsRepository metricsRepository, VeniceServerConfig serverConfig,
                             String storeName) {
    super(metricsRepository, storeName);
    totalBytesConsumedSensor = registerSensor("bytes_consumed", new Rate());
    totalRecordsConsumedSensor = registerSensor("records_consumed", new Rate());

    bytesReadFromKafkaAsUncompressedSizeSensor = registerSensor("bytes_read_from_kafka_as_uncompressed_size", new Rate(), new Total());
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
        TehutiUtils.getPercentileStat(getName() + AbstractVeniceStats.DELIMITER + keySizeSensorName));

    String valueSizeSensorName = "record_value_size_in_bytes";
    valueSizeSensor = registerSensor(valueSizeSensorName, new Avg(), new Min(), new Max(),
        TehutiUtils.getPercentileStat(getName() + AbstractVeniceStats.DELIMITER + valueSizeSensorName));

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

    String storageEnginePutLatencySensorName = "storage_engine_put_latency";
    storageEnginePutLatencySensor = registerSensor(storageEnginePutLatencySensorName, new Avg(), new Max(),
        TehutiUtils.getPercentileStat(getName() + AbstractVeniceStats.DELIMITER + storageEnginePutLatencySensorName));
    produceToDrainerQueueCallCountSensor = registerSensor("produce_to_drainer_queue_call_count", new Rate());
    produceToDrainerQueueRecordNumSensor = registerSensor("produce_to_drainer_queue_record_num", new Avg(), new Max());

    produceToKafkaRecordNumSensor = registerSensor("produce_to_kafka_record_num", new Avg(), new Max());

    /**
     * This measures the latency of producing message to local kafka VT. This will be recorded only in Leader SN for a partition.
     * Also this reports a sum of latency after processing a batch of messages, so this metric doesn't indicate the producer to broker
     * latency for each message.
     */
    produceToKafkaLatencySensor = registerSensor("produce_to_kafka_latency", new Avg(), new Max());

    writeComputeCacheHitCount = registerSensor("write_compute_cache_hit_count", new Avg(), new Max());

    totalLeaderBytesConsumedSensor = registerSensor("leader_bytes_consumed", new Rate());
    totalLeaderRecordsConsumedSensor = registerSensor("leader_records_consumed", new Rate());
    totalFollowerBytesConsumedSensor = registerSensor("follower_bytes_consumed", new Rate());
    totalFollowerRecordsConsumedSensor = registerSensor("follower_records_consumed", new Rate());
    totalLeaderBytesProducedSensor = registerSensor("leader_bytes_produced", new Rate());
    totalLeaderRecordsProducedSensor = registerSensor("leader_records_produced", new Rate());
    totalRegionHybridBytesConsumedMap = new HashMap<>();
    totalRegionHybridRecordsConsumedMap = new HashMap<>();
    for (Map.Entry<Integer, String> entry : serverConfig.getKafkaClusterIdToAliasMap().entrySet()) {
      String regionNamePrefix = RegionUtils.getRegionSpecificMetricPrefix(serverConfig.getRegionName(), entry.getValue());
      totalRegionHybridBytesConsumedMap.put(entry.getKey(), registerSensor(regionNamePrefix + "_rt_bytes_consumed", new Rate()));
      totalRegionHybridRecordsConsumedMap.put(entry.getKey(), registerSensor(regionNamePrefix + "_rt_records_consumed", new Rate()));
    }

    checksumVerificationFailureSensor = registerSensor("checksum_verification_failure", new Count());

    leaderIngestionValueBytesLookUpLatencySensor = registerSensor("leader_ingestion_value_bytes_lookup_latency", new Avg(), new Max());
    leaderIngestionValueBytesCacheHitCount = registerSensor("leader_ingestion_value_bytes_cache_hit_count", new Rate());
    leaderIngestionReplicationMetadataCacheHitCount = registerSensor("leader_ingestion_replication_metadata_cache_hit_count", new Rate());
    leaderIngestionReplicationMetadataLookUpLatencySensor = registerSensor("leader_ingestion_replication_metadata_lookup_latency", new Avg(), new Max());
    updateIgnoredDCRSensor = registerSensor("update_ignored_dcr", new Rate());
    tombstoneCreationDCRSensor = registerSensor("tombstone_creation_dcr", new Rate());

    timestampRegresssionDCRErrorRate = registerSensor("timestamp_regression_dcr_error", new Rate());
    offsetRegressionDCRErrorRate = registerSensor("offset_regression_dcr_error", new Rate());

    leaderDelegateRealTimeRecordLatencySensor = registerSensor("leader_delegate_real_time_record_latency", new Avg(), new Max());
  }

  public void recordTotalBytesConsumed(long bytes) {
    totalBytesConsumedSensor.record(bytes);
  }

  public void recordTotalRecordsConsumed(int count) {
    totalRecordsConsumedSensor.record(count);
  }

  public void recordBytesReadFromKafkaAsUncompressedSize(long bytes) {
    bytesReadFromKafkaAsUncompressedSizeSensor.record(bytes);
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

  public void recordIngestionValueBytesLookUpLatency(double latency) {
    leaderIngestionValueBytesLookUpLatencySensor.record(latency);
  }

  public void recordIngestionValueBytesCacheHitCount() {
    leaderIngestionValueBytesCacheHitCount.record();
  }

  public void recordIngestionReplicationMetadataLookUpLatency(double latency) {
    leaderIngestionReplicationMetadataLookUpLatencySensor.record(latency);
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

  public void recordProduceToKafkaRecordNum(int recordNum) {
    produceToKafkaRecordNumSensor.record(recordNum);
  }

  public void recordProduceToKafkaLatency(double latency) {
    produceToKafkaLatencySensor.record(latency);
  }

  public void recordWriteComputeCacheHitCount() {
    writeComputeCacheHitCount.record();
  }

  public void recordIngestionReplicationMetadataCacheHitCount() {
    leaderIngestionReplicationMetadataCacheHitCount.record();
  }

  public void recodUpdateIgnoredDCR() {
    updateIgnoredDCRSensor.record();
  }

  public void recorTombstoneCreatedDCR() {
    tombstoneCreationDCRSensor.record();
  }

  public void recordTotalLeaderBytesConsumed(long bytes) {
    totalLeaderBytesConsumedSensor.record(bytes);
  }

  public void recordTotalLeaderRecordsConsumed(int count) {
    totalLeaderRecordsConsumedSensor.record(count);
  }

  public void recordTotalFollowerBytesConsumed(long bytes) {
    totalFollowerBytesConsumedSensor.record(bytes);
  }

  public void recordTotalFollowerRecordsConsumed(int count) {
    totalFollowerRecordsConsumedSensor.record(count);
  }

  public void recordTotalRegionHybridBytesConsumed(int regionId, long bytes) {
    if (totalRegionHybridBytesConsumedMap.containsKey(regionId)) {
      totalRegionHybridBytesConsumedMap.get(regionId).record(bytes);
    }
  }

  public void recordTotalRegionHybridRecordsConsumed(int regionId, int count) {
    if (totalRegionHybridRecordsConsumedMap.containsKey(regionId)) {
      totalRegionHybridRecordsConsumedMap.get(regionId).record(count);
    }
  }

  public void recordTotalLeaderBytesProduced(long bytes) {
    totalLeaderBytesProducedSensor.record(bytes);
  }

  public void recordTotalLeaderRecordsProduced(int count) {
    totalLeaderRecordsProducedSensor.record(count);
  }

  public void recordChecksumVerificationFailure() {
    checksumVerificationFailureSensor.record();
  }

  public void recordTimeStampRegressionDCRError() {
    timestampRegresssionDCRErrorRate.record();
  }

  public void recordOffsetRegressionDCRError() {
    offsetRegressionDCRErrorRate.record();
  }

  public void recordLeaderDelegateRealTimeRecordLatency(double latency) {
    leaderDelegateRealTimeRecordLatencySensor.record(latency);
  }
}
