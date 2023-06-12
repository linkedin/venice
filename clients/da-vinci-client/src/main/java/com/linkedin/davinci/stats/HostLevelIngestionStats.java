package com.linkedin.davinci.stats;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.kafka.consumer.PartitionConsumptionState;
import com.linkedin.davinci.kafka.consumer.StoreIngestionTask;
import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.Gauge;
import com.linkedin.venice.stats.LongAdderRateGauge;
import com.linkedin.venice.stats.TehutiUtils;
import com.linkedin.venice.utils.RegionUtils;
import com.linkedin.venice.utils.Time;
import io.tehuti.metrics.MeasurableStat;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Count;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.Min;
import io.tehuti.metrics.stats.OccurrenceRate;
import io.tehuti.metrics.stats.Rate;
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
  private final Time time;
  // The aggregated bytes ingested rate for the entire host
  private final LongAdderRateGauge totalBytesConsumedRate;
  // The aggregated records ingested rate for the entire host
  private final LongAdderRateGauge totalRecordsConsumedRate;

  /*
   * Bytes read from Kafka by store ingestion task as a total. This metric includes bytes read for all store versions
   * allocated in a storage node reported with its uncompressed data size.
   */
  private final LongAdderRateGauge totalBytesReadFromKafkaAsUncompressedSizeRate;
  private final Sensor storageQuotaUsedSensor;
  // disk quota allowed for a store without replication. It should be as a straight line unless we bumps the disk quota
  // allowed.
  private final Sensor diskQuotaSensor;

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
  private final LongAdderRateGauge totalTimestampRegressionDCRErrorRate;
  private final LongAdderRateGauge totalOffsetRegressionDCRErrorRate;
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

  private final LongAdderRateGauge totalLeaderBytesConsumedRate;
  private final LongAdderRateGauge totalLeaderRecordsConsumedRate;
  private final LongAdderRateGauge totalFollowerBytesConsumedRate;
  private final LongAdderRateGauge totalFollowerRecordsConsumedRate;
  private final LongAdderRateGauge totalLeaderBytesProducedRate;
  private final LongAdderRateGauge totalLeaderRecordsProducedRate;
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
  private final LongAdderRateGauge totalUpdateIgnoredDCRRate;

  /**
   * Measure the count of tombstones created
   */
  private final LongAdderRateGauge totalTombstoneCreationDCRRate;

  private Sensor registerPerStoreAndTotalSensor(
      String sensorName,
      HostLevelIngestionStats totalStats,
      Supplier<Sensor> totalSensor,
      MeasurableStat... stats) {
    Sensor[] parent = totalStats == null ? null : new Sensor[] { totalSensor.get() };
    return registerSensor(sensorName, parent, stats);
  }

  private LongAdderRateGauge registerOnlyTotalRate(
      String sensorName,
      HostLevelIngestionStats totalStats,
      Supplier<LongAdderRateGauge> totalSensor) {
    if (totalStats == null) {
      LongAdderRateGauge longAdderRateGauge = new LongAdderRateGauge(time);
      registerSensor(sensorName, longAdderRateGauge);
      return longAdderRateGauge;
    } else {
      return totalSensor.get();
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
      Map<String, StoreIngestionTask> ingestionTaskMap,
      Time time) {
    super(metricsRepository, storeName);
    this.time = time;

    // Stats which are total only:

    this.totalBytesConsumedRate =
        registerOnlyTotalRate("bytes_consumed", totalStats, () -> totalStats.totalBytesConsumedRate);

    this.totalRecordsConsumedRate =
        registerOnlyTotalRate("records_consumed", totalStats, () -> totalStats.totalRecordsConsumedRate);

    this.totalBytesReadFromKafkaAsUncompressedSizeRate = registerOnlyTotalRate(
        "bytes_read_from_kafka_as_uncompressed_size",
        totalStats,
        () -> totalStats.totalBytesReadFromKafkaAsUncompressedSizeRate);

    this.totalLeaderBytesConsumedRate =
        registerOnlyTotalRate("leader_bytes_consumed", totalStats, () -> totalStats.totalLeaderBytesConsumedRate);

    this.totalLeaderRecordsConsumedRate =
        registerOnlyTotalRate("leader_records_consumed", totalStats, () -> totalStats.totalLeaderRecordsConsumedRate);

    this.totalFollowerBytesConsumedRate =
        registerOnlyTotalRate("follower_bytes_consumed", totalStats, () -> totalStats.totalFollowerBytesConsumedRate);

    this.totalFollowerRecordsConsumedRate = registerOnlyTotalRate(
        "follower_records_consumed",
        totalStats,
        () -> totalStats.totalFollowerRecordsConsumedRate);

    this.totalLeaderBytesProducedRate =
        registerOnlyTotalRate("leader_bytes_produced", totalStats, () -> totalStats.totalLeaderBytesProducedRate);

    this.totalLeaderRecordsProducedRate =
        registerOnlyTotalRate("leader_records_produced", totalStats, () -> totalStats.totalLeaderRecordsProducedRate);

    this.totalUpdateIgnoredDCRRate =
        registerOnlyTotalRate("update_ignored_dcr", totalStats, () -> totalStats.totalUpdateIgnoredDCRRate);

    this.totalTombstoneCreationDCRRate =
        registerOnlyTotalRate("tombstone_creation_dcr", totalStats, () -> totalStats.totalTombstoneCreationDCRRate);

    this.totalTimestampRegressionDCRErrorRate = registerOnlyTotalRate(
        "timestamp_regression_dcr_error",
        totalStats,
        () -> totalStats.totalTimestampRegressionDCRErrorRate);

    this.totalOffsetRegressionDCRErrorRate = registerOnlyTotalRate(
        "offset_regression_dcr_error",
        totalStats,
        () -> totalStats.totalOffsetRegressionDCRErrorRate);

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
    registerSensor(
        "ingestion_stuck_by_memory_constraint",
        new Gauge(
            () -> ingestionTaskMap.values()
                .stream()
                .filter(task -> isTotalStats ? true : task.getStoreName().equals(storeName))
                .mapToLong(task -> task.isStuckByMemoryConstraint() ? 1 : 0)
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

    this.consumerRecordsQueuePutLatencySensor = registerPerStoreAndTotalSensor(
        "consumer_records_queue_put_latency",
        totalStats,
        () -> totalStats.consumerRecordsQueuePutLatencySensor,
        avgAndMax());

    this.unexpectedMessageSensor = registerPerStoreAndTotalSensor(
        "unexpected_message",
        totalStats,
        () -> totalStats.unexpectedMessageSensor,
        new Rate());

    this.inconsistentStoreMetadataSensor = registerPerStoreAndTotalSensor(
        "inconsistent_store_metadata",
        totalStats,
        () -> totalStats.inconsistentStoreMetadataSensor,
        new Count());

    this.ingestionFailureSensor = registerPerStoreAndTotalSensor(
        "ingestion_failure",
        totalStats,
        () -> totalStats.ingestionFailureSensor,
        new Count());

    this.leaderProducerSynchronizeLatencySensor = registerPerStoreAndTotalSensor(
        "leader_producer_synchronize_latency",
        totalStats,
        () -> totalStats.leaderProducerSynchronizeLatencySensor,
        avgAndMax());

    this.leaderWriteComputeLookUpLatencySensor = registerPerStoreAndTotalSensor(
        "leader_write_compute_lookup_latency",
        totalStats,
        () -> totalStats.leaderWriteComputeLookUpLatencySensor,
        avgAndMax());

    this.leaderWriteComputeUpdateLatencySensor = registerPerStoreAndTotalSensor(
        "leader_write_compute_update_latency",
        totalStats,
        () -> totalStats.leaderWriteComputeUpdateLatencySensor,
        avgAndMax());

    this.processConsumerActionLatencySensor = registerPerStoreAndTotalSensor(
        "process_consumer_actions_latency",
        totalStats,
        () -> totalStats.processConsumerActionLatencySensor,
        avgAndMax());

    this.checkLongRunningTasksLatencySensor = registerPerStoreAndTotalSensor(
        "check_long_running_task_latency",
        totalStats,
        () -> totalStats.checkLongRunningTasksLatencySensor,
        avgAndMax());

    String storageEnginePutLatencySensorName = "storage_engine_put_latency";
    this.storageEnginePutLatencySensor = registerPerStoreAndTotalSensor(
        storageEnginePutLatencySensorName,
        totalStats,
        () -> totalStats.storageEnginePutLatencySensor,
        new Avg(),
        new Max(),
        TehutiUtils.getPercentileStat(getName() + AbstractVeniceStats.DELIMITER + storageEnginePutLatencySensorName));

    this.writeComputeCacheHitCount = registerPerStoreAndTotalSensor(
        "write_compute_cache_hit_count",
        totalStats,
        () -> totalStats.writeComputeCacheHitCount,
        new OccurrenceRate());

    this.checksumVerificationFailureSensor = registerPerStoreAndTotalSensor(
        "checksum_verification_failure",
        totalStats,
        () -> totalStats.checksumVerificationFailureSensor,
        new Count());

    this.leaderIngestionValueBytesLookUpLatencySensor = registerPerStoreAndTotalSensor(
        "leader_ingestion_value_bytes_lookup_latency",
        totalStats,
        () -> totalStats.leaderIngestionValueBytesLookUpLatencySensor,
        avgAndMax());

    this.leaderIngestionValueBytesCacheHitCount = registerPerStoreAndTotalSensor(
        "leader_ingestion_value_bytes_cache_hit_count",
        totalStats,
        () -> totalStats.leaderIngestionValueBytesCacheHitCount,
        new Rate());

    this.leaderIngestionReplicationMetadataCacheHitCount = registerPerStoreAndTotalSensor(
        "leader_ingestion_replication_metadata_cache_hit_count",
        totalStats,
        () -> totalStats.leaderIngestionReplicationMetadataCacheHitCount,
        new Rate());

    this.leaderIngestionReplicationMetadataLookUpLatencySensor = registerPerStoreAndTotalSensor(
        "leader_ingestion_replication_metadata_lookup_latency",
        totalStats,
        () -> totalStats.leaderIngestionReplicationMetadataLookUpLatencySensor,
        avgAndMax());
  }

  /** Record a host-level byte consumption rate across all store versions */
  public void recordTotalBytesConsumed(long bytes) {
    totalBytesConsumedRate.record(bytes);
  }

  /** Record a host-level record consumption rate across all store versions */
  public void recordTotalRecordsConsumed() {
    totalRecordsConsumedRate.record();
  }

  public void recordTotalBytesReadFromKafkaAsUncompressedSize(long bytes) {
    totalBytesReadFromKafkaAsUncompressedSizeRate.record(bytes);
  }

  public void recordStorageQuotaUsed(double quotaUsed, long currentTimeMs) {
    hybridQuotaUsageGauge = quotaUsed;
    storageQuotaUsedSensor.record(quotaUsed, currentTimeMs);
  }

  public void recordDiskQuotaAllowed(long quotaAllowed, long currentTimeMs) {
    diskQuotaAllowedGauge = quotaAllowed;
    diskQuotaSensor.record(quotaAllowed, currentTimeMs);
  }

  public void recordConsumerRecordsQueuePutLatency(double latency, long currentTimeMs) {
    consumerRecordsQueuePutLatencySensor.record(latency, currentTimeMs);
  }

  public void recordUnexpectedMessage() {
    unexpectedMessageSensor.record();
  }

  public void recordInconsistentStoreMetadata() {
    inconsistentStoreMetadataSensor.record();
  }

  public void recordKeySize(long bytes, long currentTimeMs) {
    keySizeSensor.record(bytes, currentTimeMs);
  }

  public void recordValueSize(long bytes, long currentTimeMs) {
    valueSizeSensor.record(bytes, currentTimeMs);
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

  public void recordIngestionValueBytesLookUpLatency(double latency, long currentTime) {
    leaderIngestionValueBytesLookUpLatencySensor.record(latency, currentTime);
  }

  public void recordIngestionValueBytesCacheHitCount(long currentTime) {
    leaderIngestionValueBytesCacheHitCount.record(1, currentTime);
  }

  public void recordIngestionReplicationMetadataLookUpLatency(double latency, long currentTimeMs) {
    leaderIngestionReplicationMetadataLookUpLatencySensor.record(latency, currentTimeMs);
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

  public void recordStorageEnginePutLatency(double latency, long currentTimeMs) {
    storageEnginePutLatencySensor.record(latency, currentTimeMs);
  }

  public void recordWriteComputeCacheHitCount() {
    writeComputeCacheHitCount.record();
  }

  public void recordIngestionReplicationMetadataCacheHitCount(long currentTimeMs) {
    leaderIngestionReplicationMetadataCacheHitCount.record(1, currentTimeMs);
  }

  public void recordUpdateIgnoredDCR() {
    totalUpdateIgnoredDCRRate.record();
  }

  public void recordTombstoneCreatedDCR() {
    totalTombstoneCreationDCRRate.record();
  }

  public void recordTotalLeaderBytesConsumed(long bytes) {
    totalLeaderBytesConsumedRate.record(bytes);
  }

  public void recordTotalLeaderRecordsConsumed() {
    totalLeaderRecordsConsumedRate.record();
  }

  public void recordTotalFollowerBytesConsumed(long bytes) {
    totalFollowerBytesConsumedRate.record(bytes);
  }

  public void recordTotalFollowerRecordsConsumed() {
    totalFollowerRecordsConsumedRate.record();
  }

  public void recordTotalRegionHybridBytesConsumed(int regionId, long bytes, long currentTimeMs) {
    Sensor sensor = totalHybridBytesConsumedByRegionId.get(regionId);
    if (sensor != null) {
      sensor.record(bytes, currentTimeMs);
    }

    sensor = totalHybridRecordsConsumedByRegionId.get(regionId);
    if (sensor != null) {
      sensor.record(1, currentTimeMs);
    }
  }

  public void recordTotalLeaderBytesProduced(long bytes) {
    totalLeaderBytesProducedRate.record(bytes);
  }

  public void recordTotalLeaderRecordsProduced(int count) {
    totalLeaderRecordsProducedRate.record(count);
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
}
