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
  protected static final String TRANSFORMER_ERROR_COUNT = "transformer_error_count";
  public static final String NEARLINE_PRODUCER_TO_LOCAL_BROKER_LATENCY = "nearline_producer_to_local_broker_latency";
  public static final String NEARLINE_LOCAL_BROKER_TO_READY_TO_SERVE_LATENCY =
      "nearline_local_broker_to_ready_to_serve_latency";
  public static final String TRANSFORMER_LATENCY = "transformer_latency";
  public static final String TRANSFORMER_LIFECYCLE_START_LATENCY = "transformer_lifecycle_start_latency";
  public static final String TRANSFORMER_LIFECYCLE_END_LATENCY = "transformer_lifecycle_end_latency";
  public static final String IDLE_TIME = "idle_time";
  public static final String PRODUCER_CALLBACK_LATENCY = "producer_callback_latency";
  public static final String LEADER_PREPROCESSING_LATENCY = "leader_preprocessing_latency";
  public static final String INTERNAL_PREPROCESSING_LATENCY = "internal_preprocessing_latency";
  public static final String BATCH_PROCESSING_REQUEST = "batch_processing_request";
  public static final String BATCH_PROCESSING_REQUEST_SIZE = "batch_processing_request_size";
  public static final String BATCH_PROCESSING_REQUEST_RECORDS = "batch_processing_request_records";
  public static final String BATCH_PROCESSING_REQUEST_LATENCY = "batch_processing_request_latency";
  public static final String BATCH_PROCESSING_REQUEST_ERROR = "batch_processing_request_error";

  private static final MetricConfig METRIC_CONFIG = new MetricConfig();
  private StoreIngestionTask ingestionTask;
  private int ingestionTaskPushTimeoutGauge = 0;
  private final Int2ObjectMap<Rate> regionIdToHybridBytesConsumedRateMap;
  private final Int2ObjectMap<Rate> regionIdToHybridRecordsConsumedRateMap;
  private final Int2ObjectMap<Avg> regionIdToHybridAvgConsumedOffsetMap;
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

  // write path latency sensors
  private final WritePathLatencySensor producerSourceBrokerLatencySensor;
  private final WritePathLatencySensor sourceBrokerLeaderConsumerLatencySensor;
  private final WritePathLatencySensor producerLocalBrokerLatencySensor;
  private final WritePathLatencySensor localBrokerFollowerConsumerLatencySensor;
  private final WritePathLatencySensor leaderProducerCompletionLatencySensor;
  private final WritePathLatencySensor subscribePrepLatencySensor;
  private final WritePathLatencySensor consumedRecordEndToEndProcessingLatencySensor;
  private final WritePathLatencySensor nearlineProducerToLocalBrokerLatencySensor;
  private final WritePathLatencySensor nearlineLocalBrokerToReadyToServeLatencySensor;
  private WritePathLatencySensor transformerLatencySensor;
  private WritePathLatencySensor transformerLifecycleStartLatencySensor;
  private WritePathLatencySensor transformerLifecycleEndLatencySensor;
  private final WritePathLatencySensor producerCallBackLatency;
  private final WritePathLatencySensor leaderPreprocessingLatency;
  private final WritePathLatencySensor internalPreprocessingLatency;
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
  private final MetricsRepository localMetricRepository;

  // Measure the max idle time among partitions for a given the store on this host
  private final LongAdderRateGauge idleTimeSensor = new LongAdderRateGauge();

  private Count transformerErrorCount = new Count();
  private Sensor transformerErrorSensor;
  private final LongAdderRateGauge batchProcessingRequestSensor = new LongAdderRateGauge();
  private final WritePathLatencySensor batchProcessingRequestSizeSensor;
  private final LongAdderRateGauge batchProcessingRequestRecordsSensor = new LongAdderRateGauge();
  private final WritePathLatencySensor batchProcessingRequestLatencySensor;
  private final LongAdderRateGauge batchProcessingRequestErrorSensor = new LongAdderRateGauge();

  public IngestionStats(VeniceServerConfig serverConfig) {

    Int2ObjectMap<String> kafkaClusterIdToAliasMap = serverConfig.getKafkaClusterIdToAliasMap();
    regionIdToHybridBytesConsumedRateMap = new Int2ObjectArrayMap<>(kafkaClusterIdToAliasMap.size());
    regionIdToHybridBytesConsumedSensorMap = new Int2ObjectArrayMap<>(kafkaClusterIdToAliasMap.size());
    regionIdToHybridRecordsConsumedRateMap = new Int2ObjectArrayMap<>(kafkaClusterIdToAliasMap.size());
    regionIdToHybridRecordsConsumedSensorMap = new Int2ObjectArrayMap<>(kafkaClusterIdToAliasMap.size());
    regionIdToHybridAvgConsumedOffsetMap = new Int2ObjectArrayMap<>(kafkaClusterIdToAliasMap.size());
    regionIdToHybridAvgConsumedOffsetSensorMap = new Int2ObjectArrayMap<>(kafkaClusterIdToAliasMap.size());

    localMetricRepository = new MetricsRepository(METRIC_CONFIG);
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

    versionTopicEndOffsetRewindSensor = localMetricRepository.sensor(VERSION_TOPIC_END_OFFSET_REWIND_COUNT);
    versionTopicEndOffsetRewindSensor.add(VERSION_TOPIC_END_OFFSET_REWIND_COUNT, versionTopicEndOffsetRewindCount);

    producerSourceBrokerLatencySensor =
        new WritePathLatencySensor(localMetricRepository, METRIC_CONFIG, "producer_to_source_broker_latency");
    sourceBrokerLeaderConsumerLatencySensor =
        new WritePathLatencySensor(localMetricRepository, METRIC_CONFIG, "source_broker_to_leader_consumer_latency");
    producerLocalBrokerLatencySensor =
        new WritePathLatencySensor(localMetricRepository, METRIC_CONFIG, "producer_to_local_broker_latency");
    localBrokerFollowerConsumerLatencySensor =
        new WritePathLatencySensor(localMetricRepository, METRIC_CONFIG, "local_broker_to_follower_consumer_latency");
    leaderProducerCompletionLatencySensor =
        new WritePathLatencySensor(localMetricRepository, METRIC_CONFIG, "leader_producer_completion_latency");
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
    producerCallBackLatency =
        new WritePathLatencySensor(localMetricRepository, METRIC_CONFIG, PRODUCER_CALLBACK_LATENCY);
    leaderPreprocessingLatency =
        new WritePathLatencySensor(localMetricRepository, METRIC_CONFIG, LEADER_PREPROCESSING_LATENCY);
    internalPreprocessingLatency =
        new WritePathLatencySensor(localMetricRepository, METRIC_CONFIG, INTERNAL_PREPROCESSING_LATENCY);

    registerSensor(localMetricRepository, UPDATE_IGNORED_DCR, conflictResolutionUpdateIgnoredSensor);
    registerSensor(localMetricRepository, TOTAL_DCR, totalConflictResolutionCountSensor);
    registerSensor(localMetricRepository, TIMESTAMP_REGRESSION_DCR_ERROR, timestampRegressionDCRErrorSensor);
    registerSensor(localMetricRepository, OFFSET_REGRESSION_DCR_ERROR, offsetRegressionDCRErrorSensor);
    registerSensor(localMetricRepository, TOMBSTONE_CREATION_DCR, tombstoneCreationDCRSensor);
    registerSensor(localMetricRepository, IDLE_TIME, idleTimeSensor);

    registerSensor(localMetricRepository, BATCH_PROCESSING_REQUEST, batchProcessingRequestSensor);
    registerSensor(localMetricRepository, BATCH_PROCESSING_REQUEST_RECORDS, batchProcessingRequestRecordsSensor);
    registerSensor(localMetricRepository, BATCH_PROCESSING_REQUEST_ERROR, batchProcessingRequestErrorSensor);
    batchProcessingRequestSizeSensor =
        new WritePathLatencySensor(localMetricRepository, METRIC_CONFIG, BATCH_PROCESSING_REQUEST_SIZE);
    batchProcessingRequestLatencySensor =
        new WritePathLatencySensor(localMetricRepository, METRIC_CONFIG, BATCH_PROCESSING_REQUEST_LATENCY);
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
    int totalFailedIngestionPartitions = ingestionTask.getFailedIngestionPartitionCount();
    boolean anyCompleted = ingestionTask.hasAnyPartitionConsumptionState(PartitionConsumptionState::isComplete);
    return anyCompleted ? totalFailedIngestionPartitions : 0;
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
    if (getHybridLeaderOffsetLag() > 0 && getLeaderBytesConsumed() == 0) {
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

  public double getSubscribePrepLatencyAvg() {
    return subscribePrepLatencySensor.getAvg();
  }

  public double getSubscribePrepLatencyMax() {
    return subscribePrepLatencySensor.getMax();
  }

  public void recordSubscribePrepLatency(double value, long currentTimeMs) {
    subscribePrepLatencySensor.record(value, currentTimeMs);
  }

  public double getProducerCallBackLatencyMax() {
    return producerCallBackLatency.getMax();
  }

  public void recordProducerCallBackLatency(double value, long currentTimeMs) {
    producerCallBackLatency.record(value, currentTimeMs);
  }

  public double getLeaderPreprocessingLatencyMax() {
    return leaderPreprocessingLatency.getMax();
  }

  public double getLeaderPreprocessingLatencyAvg() {
    return leaderPreprocessingLatency.getAvg();
  }

  public void recordLeaderPreprocessingLatency(double value, long currentTimeMs) {
    leaderPreprocessingLatency.record(value, currentTimeMs);
  }

  public double getInternalPreprocessingLatencyAvg() {
    return internalPreprocessingLatency.getAvg();
  }

  public double getInternalPreprocessingLatencyMax() {
    return internalPreprocessingLatency.getMax();
  }

  public void recordInternalPreprocessingLatency(double value, long currentTimeMs) {
    internalPreprocessingLatency.record(value, currentTimeMs);
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
    return unAvailableToZero(nearlineProducerToLocalBrokerLatencySensor.getAvg());
  }

  public double getNearlineProducerToLocalBrokerLatencyMax() {
    return unAvailableToZero(nearlineProducerToLocalBrokerLatencySensor.getMax());
  }

  public double getNearlineLocalBrokerToReadyToServeLatencyAvg() {
    return unAvailableToZero(nearlineLocalBrokerToReadyToServeLatencySensor.getAvg());
  }

  public double getNearlineLocalBrokerToReadyToServeLatencyMax() {
    return unAvailableToZero(nearlineLocalBrokerToReadyToServeLatencySensor.getMax());
  }

  public void recordNearlineProducerToLocalBrokerLatency(double value, long currentTimeMs) {
    nearlineProducerToLocalBrokerLatencySensor.record(value, currentTimeMs);
  }

  public void recordNearlineLocalBrokerToReadyToServeLatency(double value, long currentTimeMs) {
    nearlineLocalBrokerToReadyToServeLatencySensor.record(value, currentTimeMs);
  }

  public void recordTransformerError(double value, long currentTimeMs) {
    transformerErrorSensor.record(value, currentTimeMs);
  }

  public void registerTransformerErrorSensor() {
    if (transformerErrorSensor == null) {
      transformerErrorSensor = localMetricRepository.sensor(TRANSFORMER_ERROR_COUNT);
      transformerErrorSensor.add(TRANSFORMER_ERROR_COUNT, transformerErrorCount);
    }
  }

  public double getTransformerErrorCount() {
    if (transformerErrorCount != null) {
      return transformerErrorCount.measure(METRIC_CONFIG, System.currentTimeMillis());
    }
    return 0;
  }

  public void recordTransformerLatency(double value, long currentTimeMs) {
    transformerLatencySensor.record(value, currentTimeMs);
  }

  public void registerTransformerLatencySensor() {
    if (transformerLatencySensor == null) {
      transformerLatencySensor = new WritePathLatencySensor(localMetricRepository, METRIC_CONFIG, TRANSFORMER_LATENCY);
    }
  }

  public void recordTransformerLifecycleStartLatency(double value, long currentTimeMs) {
    transformerLifecycleStartLatencySensor.record(value, currentTimeMs);
  }

  public void registerTransformerLifecycleStartLatencySensor() {
    if (transformerLifecycleStartLatencySensor == null) {
      transformerLifecycleStartLatencySensor =
          new WritePathLatencySensor(localMetricRepository, METRIC_CONFIG, TRANSFORMER_LIFECYCLE_START_LATENCY);
    }
  }

  public void recordTransformerLifecycleEndLatency(double value, long currentTimeMs) {
    transformerLifecycleEndLatencySensor.record(value, currentTimeMs);
  }

  public void registerTransformerLifecycleEndLatencySensor() {
    if (transformerLifecycleEndLatencySensor == null) {
      transformerLifecycleEndLatencySensor =
          new WritePathLatencySensor(localMetricRepository, METRIC_CONFIG, TRANSFORMER_LIFECYCLE_END_LATENCY);
    }
  }

  public void recordIdleTime(long value) {
    idleTimeSensor.record(value);
  }

  public double getIdleTime() {
    return idleTimeSensor.getRate();
  }

  public WritePathLatencySensor getProducerSourceBrokerLatencySensor() {
    return producerSourceBrokerLatencySensor;
  }

  public void recordProducerSourceBrokerLatencyMs(double value, long currentTimeMs) {
    producerSourceBrokerLatencySensor.record(value, currentTimeMs);
  }

  public void recordSourceBrokerLeaderConsumerLatencyMs(double value, long currentTimeMs) {
    sourceBrokerLeaderConsumerLatencySensor.record(value, currentTimeMs);
  }

  public WritePathLatencySensor getSourceBrokerLeaderConsumerLatencySensor() {
    return sourceBrokerLeaderConsumerLatencySensor;
  }

  public void recordProducerLocalBrokerLatencyMs(double value, long currentTimeMs) {
    producerLocalBrokerLatencySensor.record(value, currentTimeMs);
  }

  public WritePathLatencySensor getProducerLocalBrokerLatencySensor() {
    return producerLocalBrokerLatencySensor;
  }

  public void recordLocalBrokerFollowerConsumerLatencyMs(double value, long currentTimeMs) {
    localBrokerFollowerConsumerLatencySensor.record(value, currentTimeMs);
  }

  public WritePathLatencySensor getLocalBrokerFollowerConsumerLatencySensor() {
    return localBrokerFollowerConsumerLatencySensor;
  }

  public void recordLeaderProducerCompletionLatencyMs(double value, long currentTimeMs) {
    leaderProducerCompletionLatencySensor.record(value, currentTimeMs);
  }

  public WritePathLatencySensor getLeaderProducerCompletionLatencySensor() {
    return leaderProducerCompletionLatencySensor;
  }

  public void recordBatchProcessingRequest(int size, long currentTimeMs) {
    batchProcessingRequestSensor.record();
    batchProcessingRequestRecordsSensor.record(size);
    batchProcessingRequestSizeSensor.record(size, currentTimeMs);
  }

  public double getBatchProcessingRequest() {
    return batchProcessingRequestSensor.getRate();
  }

  public double getBatchProcessingRequestRecords() {
    return batchProcessingRequestRecordsSensor.getRate();
  }

  public void recordBatchProcessingRequestError() {
    batchProcessingRequestErrorSensor.record();
  }

  public double getBatchProcessingRequestError() {
    return batchProcessingRequestErrorSensor.getRate();
  }

  public WritePathLatencySensor getBatchProcessingRequestSizeSensor() {
    return batchProcessingRequestSizeSensor;
  }

  public void recordBatchProcessingRequestLatency(double latency, long currentTimeMs) {
    batchProcessingRequestLatencySensor.record(latency, currentTimeMs);
  }

  public WritePathLatencySensor getBatchProcessingRequestLatencySensor() {
    return batchProcessingRequestLatencySensor;
  }

  public static double unAvailableToZero(double value) {
    /* When data is unavailable, return 0 instead of NaN or Infinity. Some metrics are initialized to -INF.
     This can cause problems when metrics are aggregated. Use only when zero makes semantic sense.
    */
    return Double.isFinite(value) ? value : 0;
  }
}
