package com.linkedin.davinci.stats;

import static com.linkedin.venice.stats.StatsErrorCode.INACTIVE_STORE_INGESTION_TASK;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.kafka.consumer.PartitionConsumptionState;
import com.linkedin.davinci.kafka.consumer.StoreIngestionTask;
import com.linkedin.venice.stats.LongAdderRateGauge;
import com.linkedin.venice.utils.RegionUtils;
import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Count;
import io.tehuti.metrics.stats.Rate;
import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;


/**
 * This class contains stats for store ingestion. The stat class is used in {@link VeniceVersionedStats} to serve for
 * a single store version or total of all store versions.
 * This class does not contain reporting logic as reporting is done by the {@link IngestionStatsReporter}.
 */
public class IngestionStats {
  protected static final String INGESTION_TASK_ERROR_GAUGE = "ingestion_task_errored_gauge";
  protected static final String INGESTION_TASK_PUSH_TIMEOUT_GAUGE = "ingestion_task_push_timeout_gauge";
  protected static final String WRITE_COMPUTE_OPERATION_FAILURE = "write_compute_operation_failure";
  protected static final String FOLLOWER_OFFSET_LAG = "follower_offset_lag";
  protected static final String LEADER_OFFSET_LAG = "leader_offset_lag";
  protected static final String HYBRID_LEADER_OFFSET_LAG = "hybrid_leader_offset_lag";
  protected static final String HYBRID_FOLLOWER_OFFSET_LAG = "hybrid_follower_offset_lag";
  protected static final String BATCH_REPLICATION_LAG = "batch_replication_lag";
  protected static final String BATCH_LEADER_OFFSET_LAG = "batch_leader_offset_lag";
  protected static final String BATCH_FOLLOWER_OFFSET_LAG = "batch_follower_offset_lag";

  protected static final String RECORDS_CONSUMED_METRIC_NAME = "records_consumed";
  protected static final String BYTES_CONSUMED_METRIC_NAME = "bytes_consumed";
  protected static final String LEADER_RECORDS_CONSUMED_METRIC_NAME = "leader_records_consumed";
  protected static final String LEADER_BYTES_CONSUMED_METRIC_NAME = "leader_bytes_consumed";
  protected static final String LEADER_STALLED_HYBRID_INGESTION_METRIC_NAME = "leader_stalled_hybrid_ingestion";
  protected static final String FOLLOWER_RECORDS_CONSUMED_METRIC_NAME = "follower_records_consumed";
  protected static final String FOLLOWER_BYTES_CONSUMED_METRIC_NAME = "follower_bytes_consumed";
  protected static final String LEADER_RECORDS_PRODUCED_METRIC_NAME = "leader_records_produced";
  protected static final String LEADER_BYTES_PRODUCED_METRIC_NAME = "leader_bytes_produced";
  protected static final String STALE_PARTITIONS_WITHOUT_INGESTION_TASK_METRIC_NAME =
      "stale_partitions_without_ingestion_task";
  protected static final String SUBSCRIBE_ACTION_PREP_LATENCY = "subscribe_action_prep_latency";
  protected static final String CONSUMED_RECORD_END_TO_END_PROCESSING_LATENCY =
      "consumed_record_end_to_end_processing_latency";
  protected static final String UPDATE_IGNORED_DCR = "update_ignored_dcr";
  protected static final String TOTAL_DCR = "total_dcr";
  protected static final String TIMESTAMP_REGRESSION_DCR_ERROR = "timestamp_regression_dcr_error";
  protected static final String OFFSET_REGRESSION_DCR_ERROR = "offset_regression_dcr_error";
  protected static final String TOMBSTONE_CREATION_DCR = "tombstone_creation_dcr";
  protected static final String READY_TO_SERVE_WITH_RT_LAG_METRIC_NAME = "ready_to_serve_with_rt_lag";
  public static final String VERSION_TOPIC_END_OFFSET_REWIND_COUNT = "version_topic_end_offset_rewind_count";
  public static final String NEARLINE_PRODUCER_TO_LOCAL_BROKER_LATENCY = "nearline_producer_to_local_broker_latency";
  public static final String NEARLINE_LOCAL_BROKER_TO_READY_TO_SERVE_LATENCY =
      "nearline_local_broker_to_ready_to_serve_latency";

  private static final MetricConfig METRIC_CONFIG = new MetricConfig();
  private StoreIngestionTask ingestionTask;
  private int ingestionTaskPushTimeoutGauge = 0;
  private final Int2ObjectMap<Rate> regionIdToHybridBytesConsumedRateMap;
  private final Int2ObjectMap<Rate> regionIdToHybridRecordsConsumedRateMap;
  private final Int2ObjectMap<Avg> regionIdToHybridAvgConsumedOffsetMap;
  private final Count stalePartitionsWithoutIngestionTaskCount;
  private final LongAdderRateGauge recordsConsumedSensor = new LongAdderRateGauge();
  private final LongAdderRateGauge bytesConsumedSensor = new LongAdderRateGauge();
  private final LongAdderRateGauge leaderRecordsConsumedSensor = new LongAdderRateGauge();
  private final LongAdderRateGauge leaderBytesConsumedSensor = new LongAdderRateGauge();
  private final LongAdderRateGauge followerRecordsConsumedSensor = new LongAdderRateGauge();
  private final LongAdderRateGauge followerBytesConsumedSensor = new LongAdderRateGauge();
  private final LongAdderRateGauge leaderRecordsProducedSensor = new LongAdderRateGauge();
  private final LongAdderRateGauge leaderBytesProducedSensor = new LongAdderRateGauge();
  private final Int2ObjectMap<Sensor> regionIdToHybridBytesConsumedSensorMap;
  private final Int2ObjectMap<Sensor> regionIdToHybridRecordsConsumedSensorMap;
  private final Int2ObjectMap<Sensor> regionIdToHybridAvgConsumedOffsetSensorMap;
  private final Sensor stalePartitionsWithoutIngestionTaskSensor;
  private final WritePathLatencySensor subscribePrepLatencySensor;
  private final WritePathLatencySensor consumedRecordEndToEndProcessingLatencySensor;
  private final WritePathLatencySensor nearlineProducerToLocalBrokerLatencySensor;
  private final WritePathLatencySensor nearlineLocalBrokerToReadyToServeLatencySensor;
  // Measure the count of ignored updates due to conflict resolution
  private final LongAdderRateGauge conflictResolutionUpdateIgnoredSensor = new LongAdderRateGauge();
  // Measure the total number of incoming conflict resolutions
  private final LongAdderRateGauge totalConflictResolutionCountSensor = new LongAdderRateGauge();
  private final LongAdderRateGauge timestampRegressionDCRErrorSensor = new LongAdderRateGauge();
  private final LongAdderRateGauge offsetRegressionDCRErrorSensor = new LongAdderRateGauge();
  private final LongAdderRateGauge tombstoneCreationDCRSensor = new LongAdderRateGauge();

  /** Record a version-level offset rewind events for VTs across all stores. */
  private final Count versionTopicEndOffsetRewindCount = new Count();
  private final Sensor versionTopicEndOffsetRewindSensor;

  public IngestionStats(VeniceServerConfig serverConfig) {

    Int2ObjectMap<String> kafkaClusterIdToAliasMap = serverConfig.getKafkaClusterIdToAliasMap();
    regionIdToHybridBytesConsumedRateMap = new Int2ObjectArrayMap<>(kafkaClusterIdToAliasMap.size());
    regionIdToHybridBytesConsumedSensorMap = new Int2ObjectArrayMap<>(kafkaClusterIdToAliasMap.size());
    regionIdToHybridRecordsConsumedRateMap = new Int2ObjectArrayMap<>(kafkaClusterIdToAliasMap.size());
    regionIdToHybridRecordsConsumedSensorMap = new Int2ObjectArrayMap<>(kafkaClusterIdToAliasMap.size());
    regionIdToHybridAvgConsumedOffsetMap = new Int2ObjectArrayMap<>(kafkaClusterIdToAliasMap.size());
    regionIdToHybridAvgConsumedOffsetSensorMap = new Int2ObjectArrayMap<>(kafkaClusterIdToAliasMap.size());

    MetricsRepository localMetricRepository = new MetricsRepository(METRIC_CONFIG);
    for (Int2ObjectMap.Entry<String> entry: kafkaClusterIdToAliasMap.int2ObjectEntrySet()) {
      int regionId = entry.getIntKey();
      String regionNamePrefix =
          RegionUtils.getRegionSpecificMetricPrefix(serverConfig.getRegionName(), entry.getValue());
      Rate regionHybridBytesConsumedRate = new Rate();
      String regionHybridBytesConsumedMetricName = regionNamePrefix + "_rt_bytes_consumed";
      Sensor regionHybridBytesConsumedSensor = localMetricRepository.sensor(regionHybridBytesConsumedMetricName);
      regionHybridBytesConsumedSensor.add(
          regionHybridBytesConsumedMetricName + regionHybridBytesConsumedRate.getClass().getSimpleName(),
          regionHybridBytesConsumedRate);
      regionIdToHybridBytesConsumedRateMap.put(regionId, regionHybridBytesConsumedRate);
      regionIdToHybridBytesConsumedSensorMap.put(regionId, regionHybridBytesConsumedSensor);

      Rate regionHybridRecordsConsumedRate = new Rate();
      String regionHybridRecordsConsumedMetricName = regionNamePrefix + "_rt_records_consumed";
      Sensor regionHybridRecordsConsumedSensor = localMetricRepository.sensor(regionHybridRecordsConsumedMetricName);
      regionHybridRecordsConsumedSensor.add(
          regionHybridRecordsConsumedMetricName + regionHybridRecordsConsumedRate.getClass().getSimpleName(),
          regionHybridRecordsConsumedRate);
      regionIdToHybridRecordsConsumedRateMap.put(regionId, regionHybridRecordsConsumedRate);
      regionIdToHybridRecordsConsumedSensorMap.put(regionId, regionHybridRecordsConsumedSensor);

      Avg regionHybridAvgConsumedOffset = new Avg();
      String regionHybridAvgConsumedOffsetMetricName = regionNamePrefix + "_rt_consumed_offset";
      Sensor regionHybridAvgConsumedOffsetSensor =
          localMetricRepository.sensor(regionHybridAvgConsumedOffsetMetricName);
      regionHybridAvgConsumedOffsetSensor.add(
          regionHybridAvgConsumedOffsetMetricName + regionHybridAvgConsumedOffset.getClass().getSimpleName(),
          regionHybridAvgConsumedOffset);
      regionIdToHybridAvgConsumedOffsetMap.put(regionId, regionHybridAvgConsumedOffset);
      regionIdToHybridAvgConsumedOffsetSensorMap.put(regionId, regionHybridAvgConsumedOffsetSensor);
    }

    registerSensor(localMetricRepository, RECORDS_CONSUMED_METRIC_NAME, recordsConsumedSensor);
    registerSensor(localMetricRepository, BYTES_CONSUMED_METRIC_NAME, bytesConsumedSensor);
    registerSensor(localMetricRepository, LEADER_RECORDS_CONSUMED_METRIC_NAME, leaderRecordsConsumedSensor);
    registerSensor(localMetricRepository, LEADER_BYTES_CONSUMED_METRIC_NAME, leaderBytesConsumedSensor);
    registerSensor(localMetricRepository, FOLLOWER_BYTES_CONSUMED_METRIC_NAME, followerBytesConsumedSensor);
    registerSensor(localMetricRepository, FOLLOWER_RECORDS_CONSUMED_METRIC_NAME, followerRecordsConsumedSensor);
    registerSensor(localMetricRepository, LEADER_RECORDS_PRODUCED_METRIC_NAME, leaderRecordsProducedSensor);
    registerSensor(localMetricRepository, LEADER_BYTES_PRODUCED_METRIC_NAME, leaderBytesProducedSensor);

    stalePartitionsWithoutIngestionTaskCount = new Count();
    stalePartitionsWithoutIngestionTaskSensor =
        localMetricRepository.sensor(STALE_PARTITIONS_WITHOUT_INGESTION_TASK_METRIC_NAME);
    stalePartitionsWithoutIngestionTaskSensor.add(
        STALE_PARTITIONS_WITHOUT_INGESTION_TASK_METRIC_NAME
            + stalePartitionsWithoutIngestionTaskCount.getClass().getSimpleName(),
        stalePartitionsWithoutIngestionTaskCount);

    versionTopicEndOffsetRewindSensor = localMetricRepository.sensor(VERSION_TOPIC_END_OFFSET_REWIND_COUNT);
    versionTopicEndOffsetRewindSensor.add(VERSION_TOPIC_END_OFFSET_REWIND_COUNT, versionTopicEndOffsetRewindCount);

    subscribePrepLatencySensor =
        new WritePathLatencySensor(localMetricRepository, METRIC_CONFIG, SUBSCRIBE_ACTION_PREP_LATENCY);
    consumedRecordEndToEndProcessingLatencySensor =
        new WritePathLatencySensor(localMetricRepository, METRIC_CONFIG, CONSUMED_RECORD_END_TO_END_PROCESSING_LATENCY);
    nearlineProducerToLocalBrokerLatencySensor =
        new WritePathLatencySensor(localMetricRepository, METRIC_CONFIG, NEARLINE_PRODUCER_TO_LOCAL_BROKER_LATENCY);
    nearlineLocalBrokerToReadyToServeLatencySensor = new WritePathLatencySensor(
        localMetricRepository,
        METRIC_CONFIG,
        NEARLINE_LOCAL_BROKER_TO_READY_TO_SERVE_LATENCY);

    registerSensor(localMetricRepository, UPDATE_IGNORED_DCR, conflictResolutionUpdateIgnoredSensor);
    registerSensor(localMetricRepository, TOTAL_DCR, totalConflictResolutionCountSensor);
    registerSensor(localMetricRepository, TIMESTAMP_REGRESSION_DCR_ERROR, timestampRegressionDCRErrorSensor);
    registerSensor(localMetricRepository, OFFSET_REGRESSION_DCR_ERROR, offsetRegressionDCRErrorSensor);
    registerSensor(localMetricRepository, TOMBSTONE_CREATION_DCR, tombstoneCreationDCRSensor);
  }

  private void registerSensor(MetricsRepository localMetricRepository, String sensorName, LongAdderRateGauge gauge) {
    Sensor sensor = localMetricRepository.sensor(sensorName);
    sensor.add(sensorName + "_rate", gauge);
  }

  public StoreIngestionTask getIngestionTask() {
    return ingestionTask;
  }

  public void setIngestionTask(StoreIngestionTask ingestionTask) {
    this.ingestionTask = ingestionTask;
  }

  private boolean hasActiveIngestionTask() {
    return ingestionTask != null && ingestionTask.isRunning();
  }

  // To prevent this metric being too noisy and align with the PreNotificationCheck of reportError, this metric should
  // only be set if the ingestion task errored after EOP is received for any of the partitions.
  public int getIngestionTaskErroredGauge() {
    if (!hasActiveIngestionTask()) {
      return 0;
    }
    boolean anyErrorReported =
        ingestionTask.hasAnyPartitionConsumptionState(PartitionConsumptionState::isErrorReported);
    boolean anyCompleted = ingestionTask.hasAnyPartitionConsumptionState(PartitionConsumptionState::isComplete);
    return anyCompleted && anyErrorReported ? 1 : 0;
  }

  public long getBatchReplicationLag() {
    if (!hasActiveIngestionTask()) {
      return 0;
    }
    return ingestionTask.getBatchReplicationLag();
  }

  public long getLeaderOffsetLag() {
    if (!hasActiveIngestionTask()) {
      return 0;
    }
    return ingestionTask.getLeaderOffsetLag();
  }

  public long getBatchLeaderOffsetLag() {
    if (!hasActiveIngestionTask()) {
      return 0;
    }
    return ingestionTask.getBatchLeaderOffsetLag();
  }

  public long getHybridLeaderOffsetLag() {
    if (!hasActiveIngestionTask()) {
      return 0;
    }
    return ingestionTask.getHybridLeaderOffsetLag();
  }

  /**
   * @return This stats is usually aggregated across the nodes so that
   * we can see the overall lags between leaders and followers.
   *
   * we return 0 instead of {@link com.linkedin.venice.stats.StatsErrorCode#INACTIVE_STORE_INGESTION_TASK}
   * so the negative error code will not mess up the aggregation.
   */
  public long getFollowerOffsetLag() {
    if (!hasActiveIngestionTask()) {
      return 0;
    }
    return ingestionTask.getFollowerOffsetLag();
  }

  public long getBatchFollowerOffsetLag() {
    if (!hasActiveIngestionTask()) {
      return 0;
    }
    return ingestionTask.getBatchFollowerOffsetLag();
  }

  public long getHybridFollowerOffsetLag() {
    if (!hasActiveIngestionTask()) {
      return 0;
    }
    return ingestionTask.getHybridFollowerOffsetLag();
  }

  public long getRegionHybridOffsetLag(int regionId) {
    if (!hasActiveIngestionTask()) {
      return 0;
    }
    return ingestionTask.getRegionHybridOffsetLag(regionId);
  }

  public int getWriteComputeErrorCode() {
    if (!hasActiveIngestionTask()) {
      return INACTIVE_STORE_INGESTION_TASK.code;
    }
    return ingestionTask.getWriteComputeErrorCode();
  }

  /**
   * @return 1 if the leader offset lag is greater than 0 and not actively ingesting data, otherwise 0.
   */
  public double getLeaderStalledHybridIngestion() {
    if (!hasActiveIngestionTask()) {
      return 0;
    }
    if (getLeaderOffsetLag() > 0 && getLeaderBytesConsumed() == 0) {
      return 1;
    } else {
      return 0;
    }
  }

  public double getReadyToServeWithRTLag() {
    if (!hasActiveIngestionTask()) {
      return 0;
    }
    if (ingestionTask.isReadyToServeAnnouncedWithRTLag()) {
      return 1;
    }
    return 0;
  }

  public double getStalePartitionsWithoutIngestionTaskCount() {
    return stalePartitionsWithoutIngestionTaskCount.measure(METRIC_CONFIG, System.currentTimeMillis());
  }

  public double getSubscribePrepLatencyAvg() {
    return subscribePrepLatencySensor.getAvg();
  }

  public double getSubscribePrepLatencyMax() {
    return subscribePrepLatencySensor.getMax();
  }

  public void recordSubscribePrepLatency(double value, long currentTimeMs) {
    subscribePrepLatencySensor.record(value, currentTimeMs);
  }

  public void recordStalePartitionsWithoutIngestionTask() {
    stalePartitionsWithoutIngestionTaskSensor.record();
  }

  public void recordVersionTopicEndOffsetRewind() {
    versionTopicEndOffsetRewindSensor.record();
  }

  public double getVersionTopicEndOffsetRewindCount() {
    return versionTopicEndOffsetRewindCount.measure(METRIC_CONFIG, System.currentTimeMillis());
  }

  public double getConsumedRecordEndToEndProcessingLatencyAvg() {
    return consumedRecordEndToEndProcessingLatencySensor.getAvg();
  }

  public double getConsumedRecordEndToEndProcessingLatencyMax() {
    return consumedRecordEndToEndProcessingLatencySensor.getMax();
  }

  public void recordConsumedRecordEndToEndProcessingLatency(double value, long currentTimeMs) {
    consumedRecordEndToEndProcessingLatencySensor.record(value, currentTimeMs);
  }

  public double getRecordsConsumed() {
    return recordsConsumedSensor.getRate();
  }

  public void recordRecordsConsumed() {
    recordsConsumedSensor.record(1);
  }

  public double getBytesConsumed() {
    return bytesConsumedSensor.getRate();
  }

  public void recordBytesConsumed(long value) {
    bytesConsumedSensor.record(value);
  }

  public double getLeaderRecordsConsumed() {
    return leaderRecordsConsumedSensor.getRate();
  }

  public void recordLeaderRecordsConsumed() {
    leaderRecordsConsumedSensor.record();
  }

  public double getLeaderBytesConsumed() {
    return leaderBytesConsumedSensor.getRate();
  }

  public void recordLeaderBytesConsumed(long value) {
    leaderBytesConsumedSensor.record(value);
  }

  public double getFollowerRecordsConsumed() {
    return followerRecordsConsumedSensor.getRate();
  }

  public void recordFollowerRecordsConsumed() {
    followerRecordsConsumedSensor.record();
  }

  public double getFollowerBytesConsumed() {
    return followerBytesConsumedSensor.getRate();
  }

  public void recordFollowerBytesConsumed(long value) {
    followerBytesConsumedSensor.record(value);
  }

  public void recordUpdateIgnoredDCR() {
    conflictResolutionUpdateIgnoredSensor.record();
  }

  public void recordTotalDCR() {
    totalConflictResolutionCountSensor.record();
  }

  public void recordTimestampRegressionDCRError() {
    timestampRegressionDCRErrorSensor.record();
  }

  public void recordOffsetRegressionDCRError() {
    offsetRegressionDCRErrorSensor.record();
  }

  public void recordTombStoneCreationDCR() {
    tombstoneCreationDCRSensor.record();
  }

  public double getRegionHybridBytesConsumed(int regionId) {
    Rate rate = regionIdToHybridBytesConsumedRateMap.get(regionId);
    return rate != null ? rate.measure(METRIC_CONFIG, System.currentTimeMillis()) : 0.0;
  }

  public void recordRegionHybridBytesConsumed(int regionId, double value, long currentTimeMs) {
    Sensor sensor = regionIdToHybridBytesConsumedSensorMap.get(regionId);
    if (sensor != null) {
      sensor.record(value, currentTimeMs);
    }
  }

  public double getRegionHybridRecordsConsumed(int regionId) {
    Rate rate = regionIdToHybridRecordsConsumedRateMap.get(regionId);
    return rate != null ? rate.measure(METRIC_CONFIG, System.currentTimeMillis()) : 0.0;
  }

  public void recordRegionHybridRecordsConsumed(int regionId, double value, long currentTimeMs) {
    Sensor sensor = regionIdToHybridRecordsConsumedSensorMap.get(regionId);
    if (sensor != null) {
      sensor.record(value, currentTimeMs);
    }
  }

  public double getRegionHybridAvgConsumedOffset(int regionId) {
    Avg avg = regionIdToHybridAvgConsumedOffsetMap.get(regionId);
    return avg != null ? avg.measure(METRIC_CONFIG, System.currentTimeMillis()) : 0.0;
  }

  public void recordRegionHybridAvgConsumedOffset(int regionId, double value, long currentTimeMs) {
    Sensor sensor = regionIdToHybridAvgConsumedOffsetSensorMap.get(regionId);
    if (sensor != null) {
      sensor.record(value, currentTimeMs);
    }
  }

  public double getLeaderRecordsProduced() {
    return leaderRecordsProducedSensor.getRate();
  }

  public double getUpdateIgnoredRate() {
    return conflictResolutionUpdateIgnoredSensor.getRate();
  }

  public double getTotalDCRRate() {
    return totalConflictResolutionCountSensor.getRate();
  }

  public double getTombstoneCreationDCRRate() {
    return tombstoneCreationDCRSensor.getRate();
  }

  public double getTimestampRegressionDCRRate() {
    return timestampRegressionDCRErrorSensor.getRate();
  }

  public double getOffsetRegressionDCRRate() {
    return offsetRegressionDCRErrorSensor.getRate();
  }

  public void recordLeaderRecordsProduced(long value) {
    leaderRecordsProducedSensor.record(value);
  }

  public double getLeaderBytesProduced() {
    return leaderBytesProducedSensor.getRate();
  }

  public void recordLeaderBytesProduced(long value) {
    leaderBytesProducedSensor.record(value);
  }

  public void setIngestionTaskPushTimeoutGauge(int value) {
    ingestionTaskPushTimeoutGauge = value;
  }

  public int getIngestionTaskPushTimeoutGauge() {
    return ingestionTaskPushTimeoutGauge;
  }

  public double getNearlineProducerToLocalBrokerLatencyAvg() {
    return nearlineProducerToLocalBrokerLatencySensor.getAvg();
  }

  public double getNearlineProducerToLocalBrokerLatencyMax() {
    return nearlineProducerToLocalBrokerLatencySensor.getMax();
  }

  public double getNearlineLocalBrokerToReadyToServeLatencyAvg() {
    return nearlineLocalBrokerToReadyToServeLatencySensor.getAvg();
  }

  public double getNearlineLocalBrokerToReadyToServeLatencyMax() {
    return nearlineLocalBrokerToReadyToServeLatencySensor.getMax();
  }

  public void recordNearlineProducerToLocalBrokerLatency(double value, long currentTimeMs) {
    nearlineProducerToLocalBrokerLatencySensor.record(value, currentTimeMs);
  }

  public void recordNearlineLocalBrokerToReadyToServeLatency(double value, long currentTimeMs) {
    nearlineLocalBrokerToReadyToServeLatencySensor.record(value, currentTimeMs);
  }

}
