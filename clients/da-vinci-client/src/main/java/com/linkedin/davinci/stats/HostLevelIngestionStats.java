package com.linkedin.davinci.stats;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.kafka.consumer.PartitionConsumptionState;
import com.linkedin.davinci.kafka.consumer.StoreIngestionTask;
import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.Gauge;
import com.linkedin.venice.stats.TehutiUtils;
import com.linkedin.venice.utils.RegionUtils;
import io.tehuti.metrics.MeasurableStat;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Count;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.Min;
import io.tehuti.metrics.stats.Rate;
import io.tehuti.metrics.stats.Total;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;


/**
 * This class contains stats for stats on the storage node host level.
 * Stats inside this class can either:
 * (1) Total only: The stat indicate total number of all the stores on this host.
 * (2) Per store only: The stat is registered for each store on this host.
 * (3) Per store and total: The stat is registered for each store on this host and the total number for this host.
 */
public class HostLevelIngestionStats extends AbstractVeniceStats {
  // The aggregated bytes ingested rate for the entire host
  private final Sensor totalBytesConsumedSensor;
  // The aggregated records ingested rate for the entire host
  private final Sensor totalRecordsConsumedSensor;
  /*
   * Bytes read from Kafka by store ingestion task as a total. This metric includes bytes read for all store versions
   * allocated in a storage node reported with its uncompressed data size.
   */
  private final Sensor totalBytesReadFromKafkaAsUncompressedSizeSensor;
  private final Sensor storageQuotaUsedSensor;
  // disk quota allowed for a store without replication. It should be as a straight line unless we bumps the disk quota
  // allowed.
  private final Sensor diskQuotaSensor;

  /** Measure latency of Kafka consumer poll request and processing returned consumer records */
  private final Sensor pollRequestSensor;
  private final Sensor pollRequestLatencySensor;
  private final Sensor pollResultNumSensor;
  /** To measure 'put' latency of consumer records blocking queue */
  private final Sensor consumerRecordsQueuePutLatencySensor;
  private final Sensor keySizeSensor;
  private final Sensor valueSizeSensor;
  private final Sensor unexpectedMessageSensor;
  private final Sensor inconsistentStoreMetadataSensor;
  private final Sensor ingestionFailureSensor;
  /**
   * Sensors for emitting if/when we detect DCR violations (such as a backwards timestamp or receding offset vector)
   */
  private final Sensor totalTimestampRegressionDCRErrorRate;
  private final Sensor totalOffsetRegressionDCRErrorRate;
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
   * This measures the latency of producing message to local kafka VT. This will be recorded only in Leader SN for a
   * partition. It reports a sum of latency after processing a batch of messages, so this metric doesn't indicate the
   * producer to broker latency for each message.
   *
   * @see StoreIngestionTask#produceToStoreBufferServiceOrKafka
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
  private final List<Sensor> totalHybridBytesConsumedByRegionId;
  private final List<Sensor> totalHybridRecordsConsumedByRegionId;

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
  private final Sensor totalUpdateIgnoredDCRSensor;

  /**
   * Measure the count of tombstones created
   */
  private final Sensor totalTombstoneCreationDCRSensor;

  private final Sensor totalLeaderDelegateRealTimeRecordLatencySensor;

  private Sensor registerPerStoreAndTotal(
      String sensorName,
      HostLevelIngestionStats totalStats,
      Supplier<Sensor> totalSensor,
      MeasurableStat... stats) {
    Sensor[] parent = totalStats == null ? null : new Sensor[] { totalSensor.get() };
    return registerSensor(sensorName, parent, stats);
  }

  private Sensor registerOnlyTotal(
      String sensorName,
      HostLevelIngestionStats totalStats,
      Supplier<Sensor> totalSensor,
      MeasurableStat... stats) {
    return totalStats == null ? registerSensor(sensorName, stats) : totalSensor.get();
  }

  /**
   * @param totalStats the total stats singleton instance, or null if we are constructing the total stats
   */
  public HostLevelIngestionStats(
      MetricsRepository metricsRepository,
      VeniceServerConfig serverConfig,
      String storeName,
      HostLevelIngestionStats totalStats,
      Map<String, StoreIngestionTask> ingestionTaskMap) {
    super(metricsRepository, storeName);
    // Stats which are total only:

    this.totalBytesConsumedSensor =
        registerOnlyTotal("bytes_consumed", totalStats, () -> totalStats.totalBytesConsumedSensor, new Rate());

    this.totalRecordsConsumedSensor =
        registerOnlyTotal("records_consumed", totalStats, () -> totalStats.totalRecordsConsumedSensor, new Rate());

    this.totalBytesReadFromKafkaAsUncompressedSizeSensor = registerOnlyTotal(
        "bytes_read_from_kafka_as_uncompressed_size",
        totalStats,
        () -> totalStats.totalBytesReadFromKafkaAsUncompressedSizeSensor,
        new Rate(),
        new Total());

    this.totalLeaderBytesConsumedSensor = registerOnlyTotal(
        "leader_bytes_consumed",
        totalStats,
        () -> totalStats.totalLeaderBytesConsumedSensor,
        new Rate());

    this.totalLeaderRecordsConsumedSensor = registerOnlyTotal(
        "leader_records_consumed",
        totalStats,
        () -> totalStats.totalLeaderRecordsConsumedSensor,
        new Rate());

    this.totalFollowerBytesConsumedSensor = registerOnlyTotal(
        "follower_bytes_consumed",
        totalStats,
        () -> totalStats.totalFollowerBytesConsumedSensor,
        new Rate());

    this.totalFollowerRecordsConsumedSensor = registerOnlyTotal(
        "follower_records_consumed",
        totalStats,
        () -> totalStats.totalFollowerRecordsConsumedSensor,
        new Rate());

    this.totalLeaderBytesProducedSensor = registerOnlyTotal(
        "leader_bytes_produced",
        totalStats,
        () -> totalStats.totalLeaderBytesProducedSensor,
        new Rate());

    this.totalLeaderRecordsProducedSensor = registerOnlyTotal(
        "leader_records_produced",
        totalStats,
        () -> totalStats.totalLeaderRecordsProducedSensor,
        new Rate());

    this.totalUpdateIgnoredDCRSensor =
        registerOnlyTotal("update_ignored_dcr", totalStats, () -> totalStats.totalUpdateIgnoredDCRSensor, new Rate());

    this.totalTombstoneCreationDCRSensor = registerOnlyTotal(
        "tombstone_creation_dcr",
        totalStats,
        () -> totalStats.totalTombstoneCreationDCRSensor,
        new Rate());

    this.totalTimestampRegressionDCRErrorRate = registerOnlyTotal(
        "timestamp_regression_dcr_error",
        totalStats,
        () -> totalStats.totalTimestampRegressionDCRErrorRate,
        new Rate());

    this.totalOffsetRegressionDCRErrorRate = registerOnlyTotal(
        "offset_regression_dcr_error",
        totalStats,
        () -> totalStats.totalOffsetRegressionDCRErrorRate,
        new Rate());

    this.totalLeaderDelegateRealTimeRecordLatencySensor = registerOnlyTotal(
        "leader_delegate_real_time_record_latency",
        totalStats,
        () -> totalStats.totalLeaderDelegateRealTimeRecordLatencySensor,
        newAvgAndMax());

    Int2ObjectMap<String> kafkaClusterIdToAliasMap = serverConfig.getKafkaClusterIdToAliasMap();
    int listSize = kafkaClusterIdToAliasMap.isEmpty() ? 0 : Collections.max(kafkaClusterIdToAliasMap.keySet()) + 1;
    Sensor[] tmpTotalHybridBytesConsumedByRegionId = new Sensor[listSize];
    Sensor[] tmpTotalHybridRecordsConsumedByRegionId = new Sensor[listSize];

    for (Int2ObjectMap.Entry<String> entry: serverConfig.getKafkaClusterIdToAliasMap().int2ObjectEntrySet()) {
      String regionNamePrefix =
          RegionUtils.getRegionSpecificMetricPrefix(serverConfig.getRegionName(), entry.getValue());
      tmpTotalHybridBytesConsumedByRegionId[entry.getIntKey()] =
          registerSensor(regionNamePrefix + "_rt_bytes_consumed", new Rate());
      tmpTotalHybridRecordsConsumedByRegionId[entry.getIntKey()] =
          registerSensor(regionNamePrefix + "_rt_records_consumed", new Rate());
    }
    this.totalHybridBytesConsumedByRegionId =
        Collections.unmodifiableList(Arrays.asList(tmpTotalHybridBytesConsumedByRegionId));
    this.totalHybridRecordsConsumedByRegionId =
        Collections.unmodifiableList(Arrays.asList(tmpTotalHybridRecordsConsumedByRegionId));

    // Stats which are per-store only:

    this.diskQuotaSensor =
        registerSensor("global_store_disk_quota_allowed", new Gauge(() -> diskQuotaAllowedGauge), new Max());

    registerSensor(
        "disk_usage_in_bytes",
        new Gauge(
            () -> ingestionTaskMap.values()
                .stream()
                .mapToLong(task -> task.getStorageEngine().getStoreSizeInBytes())
                .sum()));

    registerSensor(
        "rmd_disk_usage_in_bytes",
        new Gauge(
            () -> ingestionTaskMap.values()
                .stream()
                .mapToLong(task -> task.getStorageEngine().getRMDSizeInBytes())
                .sum()));

    String keySizeSensorName = "record_key_size_in_bytes";
    this.keySizeSensor = registerSensor(
        keySizeSensorName,
        new Avg(),
        new Min(),
        new Max(),
        TehutiUtils.getPercentileStat(getName() + AbstractVeniceStats.DELIMITER + keySizeSensorName));

    String valueSizeSensorName = "record_value_size_in_bytes";
    this.valueSizeSensor = registerSensor(
        valueSizeSensorName,
        new Avg(),
        new Min(),
        new Max(),
        TehutiUtils.getPercentileStat(getName() + AbstractVeniceStats.DELIMITER + valueSizeSensorName));

    this.storageQuotaUsedSensor =
        registerSensor("storage_quota_used", new Gauge(() -> hybridQuotaUsageGauge), new Avg(), new Min(), new Max());

    // Stats which are both per-store and total:

    this.pollRequestSensor =
        registerPerStoreAndTotal("kafka_poll_request", totalStats, () -> totalStats.pollRequestSensor, new Count());

    this.pollRequestLatencySensor = registerPerStoreAndTotal(
        "kafka_poll_request_latency",
        totalStats,
        () -> totalStats.pollRequestLatencySensor,
        newAvgAndMax());

    this.pollResultNumSensor = registerPerStoreAndTotal(
        "kafka_poll_result_num",
        totalStats,
        () -> totalStats.pollResultNumSensor,
        new Avg(),
        new Total());

    this.consumerRecordsQueuePutLatencySensor = registerPerStoreAndTotal(
        "consumer_records_queue_put_latency",
        totalStats,
        () -> totalStats.consumerRecordsQueuePutLatencySensor,
        newAvgAndMax());

    this.unexpectedMessageSensor = registerPerStoreAndTotal(
        "unexpected_message",
        totalStats,
        () -> totalStats.unexpectedMessageSensor,
        new Rate());

    this.inconsistentStoreMetadataSensor = registerPerStoreAndTotal(
        "inconsistent_store_metadata",
        totalStats,
        () -> totalStats.inconsistentStoreMetadataSensor,
        new Count());

    this.ingestionFailureSensor =
        registerPerStoreAndTotal("ingestion_failure", totalStats, () -> totalStats.ingestionFailureSensor, new Count());

    this.leaderProducerSynchronizeLatencySensor = registerPerStoreAndTotal(
        "leader_producer_synchronize_latency",
        totalStats,
        () -> totalStats.leaderProducerSynchronizeLatencySensor,
        newAvgAndMax());

    this.leaderWriteComputeLookUpLatencySensor = registerPerStoreAndTotal(
        "leader_write_compute_lookup_latency",
        totalStats,
        () -> totalStats.leaderWriteComputeLookUpLatencySensor,
        newAvgAndMax());

    this.leaderWriteComputeUpdateLatencySensor = registerPerStoreAndTotal(
        "leader_write_compute_update_latency",
        totalStats,
        () -> totalStats.leaderWriteComputeUpdateLatencySensor,
        newAvgAndMax());

    this.processConsumerActionLatencySensor = registerPerStoreAndTotal(
        "process_consumer_actions_latency",
        totalStats,
        () -> totalStats.processConsumerActionLatencySensor,
        newAvgAndMax());

    this.checkLongRunningTasksLatencySensor = registerPerStoreAndTotal(
        "check_long_running_task_latency",
        totalStats,
        () -> totalStats.checkLongRunningTasksLatencySensor,
        newAvgAndMax());

    this.quotaEnforcementLatencySensor = registerPerStoreAndTotal(
        "hybrid_quota_enforcement_latency",
        totalStats,
        () -> totalStats.quotaEnforcementLatencySensor,
        newAvgAndMax());

    this.consumerToQueueLatencySensor = registerPerStoreAndTotal(
        "consumer_to_queue_latency",
        totalStats,
        () -> totalStats.quotaEnforcementLatencySensor,
        newAvgAndMax());

    String storageEnginePutLatencySensorName = "storage_engine_put_latency";
    this.storageEnginePutLatencySensor = registerPerStoreAndTotal(
        storageEnginePutLatencySensorName,
        totalStats,
        () -> totalStats.storageEnginePutLatencySensor,
        new Avg(),
        new Max(),
        TehutiUtils.getPercentileStat(getName() + AbstractVeniceStats.DELIMITER + storageEnginePutLatencySensorName));

    this.produceToDrainerQueueCallCountSensor = registerPerStoreAndTotal(
        "produce_to_drainer_queue_call_count",
        totalStats,
        () -> totalStats.produceToDrainerQueueCallCountSensor,
        new Rate());

    this.produceToDrainerQueueRecordNumSensor = registerPerStoreAndTotal(
        "produce_to_drainer_queue_record_num",
        totalStats,
        () -> totalStats.produceToDrainerQueueCallCountSensor,
        newAvgAndMax());

    this.produceToKafkaRecordNumSensor = registerPerStoreAndTotal(
        "produce_to_kafka_record_num",
        totalStats,
        () -> totalStats.produceToKafkaRecordNumSensor,
        newAvgAndMax());

    this.produceToKafkaLatencySensor = registerPerStoreAndTotal(
        "produce_to_kafka_latency",
        totalStats,
        () -> totalStats.produceToKafkaLatencySensor,
        newAvgAndMax());

    this.writeComputeCacheHitCount = registerPerStoreAndTotal(
        "write_compute_cache_hit_count",
        totalStats,
        () -> totalStats.writeComputeCacheHitCount,
        newAvgAndMax());

    this.checksumVerificationFailureSensor = registerPerStoreAndTotal(
        "checksum_verification_failure",
        totalStats,
        () -> totalStats.checksumVerificationFailureSensor,
        new Count());

    this.leaderIngestionValueBytesLookUpLatencySensor = registerPerStoreAndTotal(
        "leader_ingestion_value_bytes_lookup_latency",
        totalStats,
        () -> totalStats.leaderIngestionValueBytesLookUpLatencySensor,
        newAvgAndMax());

    this.leaderIngestionValueBytesCacheHitCount = registerPerStoreAndTotal(
        "leader_ingestion_value_bytes_cache_hit_count",
        totalStats,
        () -> totalStats.leaderIngestionValueBytesCacheHitCount,
        new Rate());

    this.leaderIngestionReplicationMetadataCacheHitCount = registerPerStoreAndTotal(
        "leader_ingestion_replication_metadata_cache_hit_count",
        totalStats,
        () -> totalStats.leaderIngestionReplicationMetadataCacheHitCount,
        new Rate());

    this.leaderIngestionReplicationMetadataLookUpLatencySensor = registerPerStoreAndTotal(
        "leader_ingestion_replication_metadata_lookup_latency",
        totalStats,
        () -> totalStats.leaderIngestionReplicationMetadataLookUpLatencySensor,
        newAvgAndMax());
  }

  /** Record a host-level byte consumption rate across all store versions */
  public void recordTotalBytesConsumed(long bytes) {
    totalBytesConsumedSensor.record(bytes);
  }

  /** Record a host-level record consumption rate across all store versions */
  public void recordTotalRecordsConsumed() {
    totalRecordsConsumedSensor.record();
  }

  public void recordTotalBytesReadFromKafkaAsUncompressedSize(long bytes) {
    totalBytesReadFromKafkaAsUncompressedSizeSensor.record(bytes);
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

  public void recordUnexpectedMessage() {
    unexpectedMessageSensor.record();
  }

  public void recordInconsistentStoreMetadata() {
    inconsistentStoreMetadataSensor.record();
  }

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

  public void recordUpdateIgnoredDCR() {
    totalUpdateIgnoredDCRSensor.record();
  }

  public void recordTombstoneCreatedDCR() {
    totalTombstoneCreationDCRSensor.record();
  }

  public void recordTotalLeaderBytesConsumed(long bytes) {
    totalLeaderBytesConsumedSensor.record(bytes);
  }

  public void recordTotalLeaderRecordsConsumed() {
    totalLeaderRecordsConsumedSensor.record();
  }

  public void recordTotalFollowerBytesConsumed(long bytes) {
    totalFollowerBytesConsumedSensor.record(bytes);
  }

  public void recordTotalFollowerRecordsConsumed() {
    totalFollowerRecordsConsumedSensor.record();
  }

  public void recordTotalRegionHybridBytesConsumed(int regionId, long bytes) {
    Sensor sensor = totalHybridBytesConsumedByRegionId.get(regionId);
    if (sensor != null) {
      sensor.record(bytes);
    }

    sensor = totalHybridRecordsConsumedByRegionId.get(regionId);
    if (sensor != null) {
      sensor.record(1);
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

  public void recordTimestampRegressionDCRError() {
    totalTimestampRegressionDCRErrorRate.record();
  }

  public void recordOffsetRegressionDCRError() {
    totalOffsetRegressionDCRErrorRate.record();
  }

  public void recordLeaderDelegateRealTimeRecordLatency(double latency) {
    totalLeaderDelegateRealTimeRecordLatencySensor.record(latency);
  }

  private MeasurableStat[] newAvgAndMax() {
    return new MeasurableStat[] { new Avg(), new Max() };
  }
}
