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
  private final LongAdderRateGauge totalBytesConsumedSensor = new LongAdderRateGauge();
  // The aggregated records ingested rate for the entire host
  private final LongAdderRateGauge totalRecordsConsumedSensor = new LongAdderRateGauge();

  /*
   * Bytes read from Kafka by store ingestion task as a total. This metric includes bytes read for all store versions
   * allocated in a storage node reported with its uncompressed data size.
   */
  private final Sensor totalBytesReadFromKafkaAsUncompressedSizeSensor;
  private final Sensor storageQuotaUsedSensor;
  // disk quota allowed for a store without replication. It should be as a straight line unless we bumps the disk quota
  // allowed.
  private final Sensor diskQuotaSensor;

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
  private final LongAdderRateGauge totalTimestampRegressionDCRErrorRate = new LongAdderRateGauge();
  private final LongAdderRateGauge totalOffsetRegressionDCRErrorRate = new LongAdderRateGauge();
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
  // Measure the latency in putting data into storage engine
  private final Sensor storageEnginePutLatencySensor;

  /**
   * Measure the number of times a record was found in {@link PartitionConsumptionState#transientRecordMap} during UPDATE
   * message processing.
   */
  private final Sensor writeComputeCacheHitCount;

  private final LongAdderRateGauge totalLeaderBytesConsumedSensor = new LongAdderRateGauge();
  private final LongAdderRateGauge totalLeaderRecordsConsumedSensor = new LongAdderRateGauge();
  private final LongAdderRateGauge totalFollowerBytesConsumedSensor = new LongAdderRateGauge();
  private final LongAdderRateGauge totalFollowerRecordsConsumedSensor = new LongAdderRateGauge();
  private final LongAdderRateGauge totalLeaderBytesProducedSensor = new LongAdderRateGauge();
  private final LongAdderRateGauge totalLeaderRecordsProducedSensor = new LongAdderRateGauge();
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
  private final LongAdderRateGauge totalUpdateIgnoredDCRSensor = new LongAdderRateGauge();

  /**
   * Measure the count of tombstones created
   */
  private final LongAdderRateGauge totalTombstoneCreationDCRSensor = new LongAdderRateGauge();

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

  private void registerOnlyTotal(String sensorName, HostLevelIngestionStats totalStats, MeasurableStat... stats) {
    if (totalStats == null) {
      registerSensor(sensorName, stats);
    }
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

    registerOnlyTotal("bytes_consumed", totalStats, totalBytesConsumedSensor);

    registerOnlyTotal("records_consumed", totalStats, totalRecordsConsumedSensor);

    this.totalBytesReadFromKafkaAsUncompressedSizeSensor = registerOnlyTotal(
        "bytes_read_from_kafka_as_uncompressed_size",
        totalStats,
        () -> totalStats.totalBytesReadFromKafkaAsUncompressedSizeSensor,
        new Rate(),
        new Total());

    registerOnlyTotal("leader_bytes_consumed", totalStats, totalLeaderBytesConsumedSensor);

    registerOnlyTotal("leader_records_consumed", totalStats, totalLeaderRecordsConsumedSensor);

    registerOnlyTotal("follower_bytes_consumed", totalStats, totalFollowerBytesConsumedSensor);

    registerOnlyTotal("follower_records_consumed", totalStats, totalFollowerRecordsConsumedSensor);

    registerOnlyTotal("leader_bytes_produced", totalStats, totalLeaderBytesProducedSensor);

    registerOnlyTotal("leader_records_produced", totalStats, totalLeaderRecordsProducedSensor);

    registerOnlyTotal("update_ignored_dcr", totalStats, totalUpdateIgnoredDCRSensor);

    registerOnlyTotal("tombstone_creation_dcr", totalStats, totalTombstoneCreationDCRSensor);

    registerOnlyTotal("timestamp_regression_dcr_error", totalStats, totalTimestampRegressionDCRErrorRate);

    registerOnlyTotal("offset_regression_dcr_error", totalStats, totalOffsetRegressionDCRErrorRate);

    this.totalLeaderDelegateRealTimeRecordLatencySensor = registerOnlyTotal(
        "leader_delegate_real_time_record_latency",
        totalStats,
        () -> totalStats.totalLeaderDelegateRealTimeRecordLatencySensor,
        avgAndMax());

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

    // Register an aggregate metric for disk_usage_in_bytes metric
    final boolean isTotalStats = isTotalStats();
    registerSensor(
        "disk_usage_in_bytes",
        new Gauge(
            () -> ingestionTaskMap.values()
                .stream()
                .filter(task -> isTotalStats ? true : task.getStoreName().equals(storeName))
                .mapToLong(
                    task -> isTotalStats
                        ? task.getStorageEngine().getCachedStoreSizeInBytes()
                        : task.getStorageEngine().getStoreSizeInBytes())
                .sum()));
    // Register an aggregate metric for rmd_disk_usage_in_bytes metric
    registerSensor(
        "rmd_disk_usage_in_bytes",
        new Gauge(
            () -> ingestionTaskMap.values()
                .stream()
                .filter(task -> isTotalStats ? true : task.getStoreName().equals(storeName))
                .mapToLong(
                    task -> isTotalStats
                        ? task.getStorageEngine().getCachedRMDSizeInBytes()
                        : task.getStorageEngine().getRMDSizeInBytes())
                .sum()));

    // Stats which are per-store only:
    this.diskQuotaSensor =
        registerSensor("global_store_disk_quota_allowed", new Gauge(() -> diskQuotaAllowedGauge), new Max());

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
        avgAndMax());

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
        avgAndMax());

    this.leaderWriteComputeLookUpLatencySensor = registerPerStoreAndTotal(
        "leader_write_compute_lookup_latency",
        totalStats,
        () -> totalStats.leaderWriteComputeLookUpLatencySensor,
        avgAndMax());

    this.leaderWriteComputeUpdateLatencySensor = registerPerStoreAndTotal(
        "leader_write_compute_update_latency",
        totalStats,
        () -> totalStats.leaderWriteComputeUpdateLatencySensor,
        avgAndMax());

    this.processConsumerActionLatencySensor = registerPerStoreAndTotal(
        "process_consumer_actions_latency",
        totalStats,
        () -> totalStats.processConsumerActionLatencySensor,
        avgAndMax());

    this.checkLongRunningTasksLatencySensor = registerPerStoreAndTotal(
        "check_long_running_task_latency",
        totalStats,
        () -> totalStats.checkLongRunningTasksLatencySensor,
        avgAndMax());

    String storageEnginePutLatencySensorName = "storage_engine_put_latency";
    this.storageEnginePutLatencySensor = registerPerStoreAndTotal(
        storageEnginePutLatencySensorName,
        totalStats,
        () -> totalStats.storageEnginePutLatencySensor,
        new Avg(),
        new Max(),
        TehutiUtils.getPercentileStat(getName() + AbstractVeniceStats.DELIMITER + storageEnginePutLatencySensorName));

    this.writeComputeCacheHitCount = registerPerStoreAndTotal(
        "write_compute_cache_hit_count",
        totalStats,
        () -> totalStats.writeComputeCacheHitCount,
        avgAndMax());

    this.checksumVerificationFailureSensor = registerPerStoreAndTotal(
        "checksum_verification_failure",
        totalStats,
        () -> totalStats.checksumVerificationFailureSensor,
        new Count());

    this.leaderIngestionValueBytesLookUpLatencySensor = registerPerStoreAndTotal(
        "leader_ingestion_value_bytes_lookup_latency",
        totalStats,
        () -> totalStats.leaderIngestionValueBytesLookUpLatencySensor,
        avgAndMax());

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
        avgAndMax());
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

  public void recordStorageEnginePutLatency(double latency) {
    storageEnginePutLatencySensor.record(latency);
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
}
