package com.linkedin.davinci.stats;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.kafka.consumer.PartitionConsumptionState;
import com.linkedin.davinci.kafka.consumer.StoreIngestionTask;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.stats.Gauge;
import com.linkedin.venice.utils.RegionUtils;
import com.linkedin.venice.utils.Utils;
import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Count;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.Rate;
import java.util.HashMap;
import java.util.Map;
import java.util.function.DoubleSupplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static com.linkedin.venice.stats.StatsErrorCode.*;


/**
 * The store level stats or the total stats will be unpopulated because there is no easy and reliable way to aggregate
 * gauge stats such as rt topic offset lag.
 */
public class AggVersionedStorageIngestionStats extends AbstractVeniceAggVersionedStats<
    AggVersionedStorageIngestionStats.StorageIngestionStats,
    AggVersionedStorageIngestionStats.StorageIngestionStatsReporter> {
  private static final Logger logger = LogManager.getLogger(AggVersionedStorageIngestionStats.class);

  private static final String RECORDS_CONSUMED_METRIC_NAME = "records_consumed";
  private static final String BYTES_CONSUMED_METRIC_NAME = "bytes_consumed";

  private static final String LEADER_RECORDS_CONSUMED_METRIC_NAME = "leader_records_consumed";
  private static final String LEADER_BYTES_CONSUMED_METRIC_NAME = "leader_bytes_consumed";
  private static final String LEADER_STALLED_HYBRID_INGESTION_METRIC_NAME = "leader_stalled_hybrid_ingestion";
  private static final String FOLLOWER_RECORDS_CONSUMED_METRIC_NAME = "follower_records_consumed";
  private static final String FOLLOWER_BYTES_CONSUMED_METRIC_NAME = "follower_bytes_consumed";
  private static final String LEADER_RECORDS_PRODUCED_METRIC_NAME = "leader_records_produced";
  private static final String LEADER_BYTES_PRODUCED_METRIC_NAME = "leader_bytes_produced";
  private static final String STALE_PARTITIONS_WITHOUT_INGESTION_TASK_METRIC_NAME = "stale_partitions_without_ingestion_task";
  private static final String SUBSCRIBE_ACTION_PREP_LATENCY = "subscribe_action_prep_latency";
  private static final String SUBSCRIBE_ACTION_GET_CONSUMER_LATENCY = "subscribe_action_get_consumer_latency";
  private static final String SUBSCRIBE_SUBSCRIBE_ACTION_CONSUMER_SUBSCRIBE_LATENCY = "subscribe_action_consumer_subscribe_latency";
  private static final String UPDATE_IGNORED_DCR = "update_ignored_dcr";
  private static final String TOTAL_DCR = "total_dcr";
  private static final String TIMESTAMP_REGRESSION_DCR_ERROR = "timestamp_regression_dcr_error";
  private static final String OFFSET_REGRESSION_DCR_ERROR = "offset_regression_dcr_error";
  private static final String TOMBSTONE_CREATION_DCR = "tombstone_creation_dcr";
  private static final String READY_TO_SERVE_WITH_RT_LAG_METRIC_NAME = "ready_to_serve_with_rt_lag";

  private static final String MAX = "_max";
  private static final String AVG = "_avg";

  public AggVersionedStorageIngestionStats(MetricsRepository metricsRepository, ReadOnlyStoreRepository storeRepository, VeniceServerConfig serverConfig) {
    super(metricsRepository, storeRepository, () -> new StorageIngestionStats(serverConfig), StorageIngestionStatsReporter::new);
  }

  public void setIngestionTask(String storeVersionTopic, StoreIngestionTask ingestionTask) {
    if (!Version.isVersionTopicOrStreamReprocessingTopic(storeVersionTopic)) {
      logger.warn("Invalid store version topic name: " + storeVersionTopic);
      return;
    }
    String storeName = Version.parseStoreFromKafkaTopicName(storeVersionTopic);
    int version = Version.parseVersionFromKafkaTopicName(storeVersionTopic);
    try {
      /**
       * Set up the ingestion task reference before registering any metrics that depend on the task reference.
       */
      getStats(storeName, version).setIngestionTask(ingestionTask);

      // Make sure the hybrid store stats are registered
      if (ingestionTask.isHybridMode()) {
        registerConditionalStats(storeName);
      }
    } catch (Exception e) {
      logger.warn("Failed to set up versioned storage ingestion stats of store: " + storeName
          + ", version: " + version);
    }
  }

  public void recordRecordsConsumed(String storeName, int version, int count) {
    Utils.computeIfNotNull(getTotalStats(storeName), stat -> stat.recordRecordsConsumed(count));
    Utils.computeIfNotNull(getStats(storeName, version), stat -> stat.recordRecordsConsumed(count));
  }

  public void recordBytesConsumed(String storeName, int version, long bytes) {
    Utils.computeIfNotNull(getTotalStats(storeName), stat -> stat.recordBytesConsumed(bytes));
    Utils.computeIfNotNull(getStats(storeName, version), stat -> stat.recordBytesConsumed(bytes));
  }

  public void recordLeaderRecordsConsumed(String storeName, int version, int count) {
    Utils.computeIfNotNull(getTotalStats(storeName), stat -> stat.recordLeaderRecordsConsumed(count));
    Utils.computeIfNotNull(getStats(storeName, version), stat -> stat.recordLeaderRecordsConsumed(count));
  }

  public void recordLeaderBytesConsumed(String storeName, int version, long bytes) {
    Utils.computeIfNotNull(getTotalStats(storeName), stat -> stat.recordLeaderBytesConsumed(bytes));
    Utils.computeIfNotNull(getStats(storeName, version), stat -> stat.recordLeaderBytesConsumed(bytes));
  }

  public void recordFollowerRecordsConsumed(String storeName, int version, int count) {
    Utils.computeIfNotNull(getTotalStats(storeName), stat -> stat.recordFollowerRecordsConsumed(count));
    Utils.computeIfNotNull(getStats(storeName, version), stat -> stat.recordFollowerRecordsConsumed(count));
  }

  public void recordFollowerBytesConsumed(String storeName, int version, long bytes) {
    Utils.computeIfNotNull(getTotalStats(storeName), stat -> stat.recordFollowerBytesConsumed(bytes));
    Utils.computeIfNotNull(getStats(storeName, version), stat -> stat.recordFollowerBytesConsumed(bytes));
  }

  public void recordLeaderRecordsProduced(String storeName, int version, int count) {
    Utils.computeIfNotNull(getTotalStats(storeName), stat -> stat.recordLeaderRecordsProduced(count));
    Utils.computeIfNotNull(getStats(storeName, version), stat -> stat.recordLeaderRecordsProduced(count));
  }

  public void recordLeaderBytesProduced(String storeName, int version, long bytes) {
    Utils.computeIfNotNull(getTotalStats(storeName), stat -> stat.recordLeaderBytesProduced(bytes));
    Utils.computeIfNotNull(getStats(storeName, version), stat -> stat.recordLeaderBytesProduced(bytes));
  }

  public void recordRegionHybridBytesConsumed(String storeName, int version, long bytes, int regionId) {
    Utils.computeIfNotNull(getTotalStats(storeName), stat -> stat.recordRegionHybridBytesConsumed(regionId, bytes));
    Utils.computeIfNotNull(getStats(storeName, version), stat -> stat.recordRegionHybridBytesConsumed(regionId, bytes));
  }

  public void recordRegionHybridRecordsConsumed(String storeName, int version, int count, int regionId) {
    Utils.computeIfNotNull(getTotalStats(storeName), stat -> stat.recordRegionHybridRecordsConsumed(regionId, count));
    Utils.computeIfNotNull(getStats(storeName, version), stat -> stat.recordRegionHybridRecordsConsumed(regionId, count));
  }

  public void recordRegionHybridAvgConsumedOffset(String storeName, int version, long offset, int regionId) {
    Utils.computeIfNotNull(getTotalStats(storeName), stat -> stat.recordRegionHybridAvgConsumedOffset(regionId, offset));
    Utils.computeIfNotNull(getStats(storeName, version), stat -> stat.recordRegionHybridAvgConsumedOffset(regionId, offset));
  }

  public void recordUpdateIgnoredDCR(String storeName, int version) {
    Utils.computeIfNotNull(getTotalStats(storeName), StorageIngestionStats::recordUpdateIgnoredDCR);
    Utils.computeIfNotNull(getStats(storeName, version), StorageIngestionStats::recordUpdateIgnoredDCR);
  }

  public void recordTotalDCR(String storeName, int version) {
    Utils.computeIfNotNull(getTotalStats(storeName), StorageIngestionStats::recordTotalDCR);
    Utils.computeIfNotNull(getStats(storeName, version), StorageIngestionStats::recordTotalDCR);
  }

  public void recordTimestampRegressionDCRError(String storeName, int version) {
    Utils.computeIfNotNull(getTotalStats(storeName), StorageIngestionStats::recordTimestampRegressionDCRError);
    Utils.computeIfNotNull(getStats(storeName, version), StorageIngestionStats::recordTimestampRegressionDCRError);
  }

  public void recordOffsetRegressionDCRError(String storeName, int version) {
    Utils.computeIfNotNull(getTotalStats(storeName), StorageIngestionStats::recordOffsetRegressionDCRError);
    Utils.computeIfNotNull(getStats(storeName, version), StorageIngestionStats::recordOffsetRegressionDCRError);
  }

  public void recordTombStoneCreationDCR(String storeName, int version) {
    Utils.computeIfNotNull(getTotalStats(storeName), StorageIngestionStats::recordTombStoneCreationDCR);
    Utils.computeIfNotNull(getStats(storeName, version), StorageIngestionStats::recordTombStoneCreationDCR);
  }

  public void setIngestionTaskPushTimeoutGauge(String storeName, int version) {
    getStats(storeName, version).setIngestionTaskPushTimeoutGauge(1);
  }

  public void resetIngestionTaskPushTimeoutGauge(String storeName, int version) {
    getStats(storeName, version).setIngestionTaskPushTimeoutGauge(0);
  }

  public void recordStalePartitionsWithoutIngestionTask(String storeName, int version) {
    Utils.computeIfNotNull(getTotalStats(storeName), StorageIngestionStats::recordStalePartitionsWithoutIngestionTask);
    Utils.computeIfNotNull(getStats(storeName, version), StorageIngestionStats::recordStalePartitionsWithoutIngestionTask);
  }

  public void recordSubscribePrepLatency(String storeName, int version, double value) {
    Utils.computeIfNotNull(getTotalStats(storeName), stat -> stat.recordSubscribePrepLatency(value));
    Utils.computeIfNotNull(getStats(storeName, version), stat -> stat.recordSubscribePrepLatency(value));
  }

  static class StorageIngestionStats {
    private static final MetricConfig METRIC_CONFIG = new MetricConfig();
    private final MetricsRepository localMetricRepository = new MetricsRepository(METRIC_CONFIG);
    private final Map<Integer, String> kafkaClusterIdToAliasMap;

    private StoreIngestionTask ingestionTask;
    private long rtTopicOffsetLagOverThreshold = 0;
    private int ingestionTaskPushTimeoutGauge = 0;

    private final Rate recordsConsumedRate;
    private final Rate bytesConsumedRate;
    private final Rate leaderRecordsConsumedRate;
    private final Rate leaderBytesConsumedRate;
    private final Rate followerRecordsConsumedRate;
    private final Rate followerBytesConsumedRate;
    private final Rate leaderRecordsProducedRate;
    private final Rate leaderBytesProducedRate;
    private final Rate updatedIgnoredDCRRate;
    private final Rate totalDCRRate;
    private final Rate timestampRegressionDCRRate;
    private final Rate offsetRegressionDCRRate;
    private final Rate tombstoneCreationDCRRate;

    private final Map<Integer, Rate> regionIdToHybridBytesConsumedRateMap;
    private final Map<Integer, Rate> regionIdToHybridRecordsConsumedRateMap;
    private final Map<Integer, Avg> regionIdToHybridAvgConsumedOffsetMap;
    private final Count stalePartitionsWithoutIngestionTaskCount;
    private final Avg subscribePrepLatencyAvg;
    private final Avg subscribeGetConsumerLatencyAvg;
    private final Avg subscribeConsumerSubscribeLatencyAvg;
    private final Max subscribePrepLatencyMax;
    private final Max subscribeGetConsumerLatencyMax;
    private final Max subscribeConsumerSubscribeLatencyMax;

    private final Sensor recordsConsumedSensor;
    private final Sensor bytesConsumedSensor;
    private final Sensor leaderRecordsConsumedSensor;
    private final Sensor leaderBytesConsumedSensor;
    private final Sensor followerRecordsConsumedSensor;
    private final Sensor followerBytesConsumedSensor;
    private final Sensor leaderRecordsProducedSensor;
    private final Sensor leaderBytesProducedSensor;
    private final Map<Integer, Sensor> regionIdToHybridBytesConsumedSensorMap;
    private final Map<Integer, Sensor> regionIdToHybridRecordsConsumedSensorMap;
    private final Map<Integer, Sensor> regionIdToHybridAvgConsumedOffsetSensorMap;
    private final Sensor stalePartitionsWithoutIngestionTaskSensor;
    private final Sensor subscribePrepLatencySensor;
    /**
     * Measure the count of ignored updates due to conflict resolution
     */
    private final Sensor conflictResolutionUpdateIgnoredSensor;
    // Measure the total number of incoming conflict resolutions
    private final Sensor totalConflictResolutionCountSensor;
    private final Sensor timestampRegressionDCRErrorSensor;
    private final Sensor offsetRegressionDCRErrorSensor;
    private final Sensor tombstoneCreationDCRSensor;

    public StorageIngestionStats(VeniceServerConfig serverConfig)  {
      kafkaClusterIdToAliasMap = serverConfig.getKafkaClusterIdToAliasMap();

      regionIdToHybridBytesConsumedRateMap = new HashMap<>();
      regionIdToHybridBytesConsumedSensorMap = new HashMap<>();
      regionIdToHybridRecordsConsumedRateMap = new HashMap<>();
      regionIdToHybridRecordsConsumedSensorMap = new HashMap<>();
      regionIdToHybridAvgConsumedOffsetMap = new HashMap<>();
      regionIdToHybridAvgConsumedOffsetSensorMap = new HashMap<>();

      for (Map.Entry<Integer, String> entry : kafkaClusterIdToAliasMap.entrySet()) {
        String regionNamePrefix = RegionUtils.getRegionSpecificMetricPrefix(serverConfig.getRegionName(), entry.getValue());
        Rate regionHybridBytesConsumedRate = new Rate();
        String regionHybridBytesConsumedMetricName = regionNamePrefix + "_rt_bytes_consumed";
        Sensor regionHybridBytesConsumedSensor = localMetricRepository.sensor(regionHybridBytesConsumedMetricName);
        regionHybridBytesConsumedSensor.add(regionHybridBytesConsumedMetricName + regionHybridBytesConsumedRate.getClass().getSimpleName(), regionHybridBytesConsumedRate);
        regionIdToHybridBytesConsumedRateMap.put(entry.getKey(),  regionHybridBytesConsumedRate);
        regionIdToHybridBytesConsumedSensorMap.put(entry.getKey(),  regionHybridBytesConsumedSensor);

        Rate regionHybridRecordsConsumedRate = new Rate();
        String regionHybridRecordsConsumedMetricName = regionNamePrefix + "_rt_records_consumed";
        Sensor regionHybridRecordsConsumedSensor = localMetricRepository.sensor(regionHybridRecordsConsumedMetricName);
        regionHybridRecordsConsumedSensor.add(regionHybridRecordsConsumedMetricName + regionHybridRecordsConsumedRate.getClass().getSimpleName(), regionHybridRecordsConsumedRate);
        regionIdToHybridRecordsConsumedRateMap.put(entry.getKey(), regionHybridRecordsConsumedRate);
        regionIdToHybridRecordsConsumedSensorMap.put(entry.getKey(), regionHybridRecordsConsumedSensor);

        Avg regionHybridAvgConsumedOffset = new Avg();
        String regionHybridAvgConsumedOffsetMetricName = regionNamePrefix + "_rt_consumed_offset";
        Sensor regionHybridAvgConsumedOffsetSensor = localMetricRepository.sensor(regionHybridAvgConsumedOffsetMetricName);
        regionHybridAvgConsumedOffsetSensor.add(regionHybridAvgConsumedOffsetMetricName + regionHybridAvgConsumedOffset.getClass().getSimpleName(), regionHybridAvgConsumedOffset);
        regionIdToHybridAvgConsumedOffsetMap.put(entry.getKey(), regionHybridAvgConsumedOffset);
        regionIdToHybridAvgConsumedOffsetSensorMap.put(entry.getKey(), regionHybridAvgConsumedOffsetSensor);
      }

      recordsConsumedRate = new Rate();
      recordsConsumedSensor = localMetricRepository.sensor(RECORDS_CONSUMED_METRIC_NAME);
      recordsConsumedSensor.add(RECORDS_CONSUMED_METRIC_NAME + recordsConsumedRate.getClass().getSimpleName(), recordsConsumedRate);

      bytesConsumedRate = new Rate();
      bytesConsumedSensor = localMetricRepository.sensor(BYTES_CONSUMED_METRIC_NAME);
      bytesConsumedSensor.add(BYTES_CONSUMED_METRIC_NAME + bytesConsumedRate.getClass().getSimpleName(), bytesConsumedRate);

      leaderRecordsConsumedRate = new Rate();
      leaderRecordsConsumedSensor = localMetricRepository.sensor(LEADER_RECORDS_CONSUMED_METRIC_NAME);
      leaderRecordsConsumedSensor.add(LEADER_RECORDS_CONSUMED_METRIC_NAME + leaderRecordsConsumedRate.getClass().getSimpleName(), leaderRecordsConsumedRate);

      leaderBytesConsumedRate = new Rate();
      leaderBytesConsumedSensor = localMetricRepository.sensor(LEADER_BYTES_CONSUMED_METRIC_NAME);
      leaderBytesConsumedSensor.add(LEADER_BYTES_CONSUMED_METRIC_NAME + leaderBytesConsumedRate.getClass().getSimpleName(), leaderBytesConsumedRate);

      followerRecordsConsumedRate = new Rate();
      followerRecordsConsumedSensor = localMetricRepository.sensor(FOLLOWER_RECORDS_CONSUMED_METRIC_NAME);
      followerRecordsConsumedSensor.add(FOLLOWER_RECORDS_CONSUMED_METRIC_NAME + followerRecordsConsumedRate.getClass().getSimpleName(), followerRecordsConsumedRate);

      followerBytesConsumedRate = new Rate();
      followerBytesConsumedSensor = localMetricRepository.sensor(FOLLOWER_BYTES_CONSUMED_METRIC_NAME);
      followerBytesConsumedSensor.add(FOLLOWER_BYTES_CONSUMED_METRIC_NAME + followerBytesConsumedRate.getClass().getSimpleName(), followerBytesConsumedRate);

      leaderRecordsProducedRate = new Rate();
      leaderRecordsProducedSensor = localMetricRepository.sensor(LEADER_RECORDS_PRODUCED_METRIC_NAME);
      leaderRecordsProducedSensor.add(LEADER_RECORDS_PRODUCED_METRIC_NAME + leaderRecordsProducedRate.getClass().getSimpleName(), leaderRecordsProducedRate);

      leaderBytesProducedRate = new Rate();
      leaderBytesProducedSensor = localMetricRepository.sensor(LEADER_BYTES_PRODUCED_METRIC_NAME);
      leaderBytesProducedSensor.add(LEADER_BYTES_PRODUCED_METRIC_NAME + leaderBytesProducedRate.getClass().getSimpleName(), leaderBytesProducedRate);

      stalePartitionsWithoutIngestionTaskCount = new Count();
      stalePartitionsWithoutIngestionTaskSensor = localMetricRepository.sensor(
          STALE_PARTITIONS_WITHOUT_INGESTION_TASK_METRIC_NAME);
      stalePartitionsWithoutIngestionTaskSensor.add(STALE_PARTITIONS_WITHOUT_INGESTION_TASK_METRIC_NAME
          + stalePartitionsWithoutIngestionTaskCount.getClass().getSimpleName(), stalePartitionsWithoutIngestionTaskCount);

      subscribePrepLatencyAvg = new Avg();
      subscribePrepLatencyMax = new Max();
      subscribePrepLatencySensor = localMetricRepository.sensor(SUBSCRIBE_ACTION_PREP_LATENCY);
      subscribePrepLatencySensor.add(SUBSCRIBE_ACTION_PREP_LATENCY
          + subscribePrepLatencyMax.getClass().getSimpleName(), subscribePrepLatencyMax);
      subscribePrepLatencySensor.add(SUBSCRIBE_ACTION_PREP_LATENCY
          + subscribePrepLatencyAvg,getClass().getSimpleName(), subscribePrepLatencyAvg);

      subscribeGetConsumerLatencyAvg = new Avg();
      subscribeGetConsumerLatencyMax = new Max();

      subscribeConsumerSubscribeLatencyAvg = new Avg();
      subscribeConsumerSubscribeLatencyMax = new Max();
      updatedIgnoredDCRRate = new Rate();
      conflictResolutionUpdateIgnoredSensor = localMetricRepository.sensor(UPDATE_IGNORED_DCR);
      conflictResolutionUpdateIgnoredSensor.add(UPDATE_IGNORED_DCR + updatedIgnoredDCRRate.getClass().getSimpleName(),
          updatedIgnoredDCRRate);

      totalDCRRate = new Rate();
      totalConflictResolutionCountSensor = localMetricRepository.sensor(TOTAL_DCR);
      totalConflictResolutionCountSensor.add(TOTAL_DCR + totalDCRRate.getClass().getSimpleName(), totalDCRRate);

      timestampRegressionDCRRate = new Rate();
      timestampRegressionDCRErrorSensor = localMetricRepository.sensor(TIMESTAMP_REGRESSION_DCR_ERROR);
      timestampRegressionDCRErrorSensor.add(TIMESTAMP_REGRESSION_DCR_ERROR + timestampRegressionDCRRate.getClass().getSimpleName(),
          timestampRegressionDCRRate);

      offsetRegressionDCRRate = new Rate();
      offsetRegressionDCRErrorSensor = localMetricRepository.sensor(OFFSET_REGRESSION_DCR_ERROR);
      offsetRegressionDCRErrorSensor.add(OFFSET_REGRESSION_DCR_ERROR + offsetRegressionDCRRate.getClass().getSimpleName(), offsetRegressionDCRRate);

      tombstoneCreationDCRRate = new Rate();
      tombstoneCreationDCRSensor = localMetricRepository.sensor(TOMBSTONE_CREATION_DCR);
      tombstoneCreationDCRSensor.add(TOMBSTONE_CREATION_DCR + tombstoneCreationDCRRate.getClass().getSimpleName(),
          tombstoneCreationDCRRate);
    }

    public void setIngestionTask(StoreIngestionTask ingestionTask) {
      this.ingestionTask = ingestionTask;
    }

    public long getRtTopicOffsetLag() {
      if (ingestionTask == null) {
        /**
         * Once a versioned stat is created on a host, it cannot be unregistered because a specific version doesn't
         * exist on the host; however, we can't guarantee every single store version will have a replica on the host.
         * In this case, ingestion task will not be created, which is not an error.
         */
        return 0;
      }
      else if (!ingestionTask.isHybridMode()) {
        rtTopicOffsetLagOverThreshold = METRIC_ONLY_AVAILABLE_FOR_HYBRID_STORES.code;
        return METRIC_ONLY_AVAILABLE_FOR_HYBRID_STORES.code;
      } else {
        // Hybrid store and store ingestion is initialized.
        long rtTopicOffsetLag = ingestionTask.getRealTimeBufferOffsetLag();
        rtTopicOffsetLagOverThreshold = Math.max(0, rtTopicOffsetLag - ingestionTask.getOffsetLagThreshold());
        return rtTopicOffsetLag;
      }
    }

    public long getNumberOfPartitionsNotReceiveSOBR() {
      if (ingestionTask == null) {
        return INACTIVE_STORE_INGESTION_TASK.code;
      }
      return ingestionTask.getNumOfPartitionsNotReceiveSOBR();
    }

    public long getRtTopicOffsetLagOverThreshold() {
      return rtTopicOffsetLagOverThreshold;
    }

    // To prevent this metric being too noisy and align with the PreNotificationCheck of reportError, this metric should
    // only be set if the ingestion task errored after EOP is received for any of the partitions.
    public int getIngestionTaskErroredGauge() {
      if (ingestionTask == null) {
        return 0;
      }
      boolean anyErrorReported = ingestionTask.hasAnyPartitionConsumptionState(
          PartitionConsumptionState::isErrorReported);
      boolean anyCompleted = ingestionTask.hasAnyPartitionConsumptionState(PartitionConsumptionState::isComplete);
      return anyCompleted && anyErrorReported ? 1 : 0;
    }

    public long getBatchReplicationLag() {
      if (ingestionTask == null) {
        return 0;
      }
      return ingestionTask.getBatchReplicationLag();
    }

    public long getLeaderOffsetLag() {
      if (ingestionTask == null) {
        return 0;
      }
      return ingestionTask.getLeaderOffsetLag();
    }

    public long getBatchLeaderOffsetLag() {
      if (ingestionTask == null) {
        return 0;
      }
      return ingestionTask.getBatchLeaderOffsetLag();
    }

    public long getHybridLeaderOffsetLag() {
      if (ingestionTask == null) {
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
      if (ingestionTask == null) {
        return 0;
      }
      return ingestionTask.getFollowerOffsetLag();
    }

    public long getBatchFollowerOffsetLag() {
      if (ingestionTask == null) {
        return 0;
      }
      return ingestionTask.getBatchFollowerOffsetLag();
    }

    public long getHybridFollowerOffsetLag() {
      if (ingestionTask == null) {
        return 0;
      }
      return ingestionTask.getHybridFollowerOffsetLag();
    }

    public long getRegionHybridOffsetLag(int regionId) {
      if (ingestionTask == null) {
        return 0;
      }
      return ingestionTask.getRegionHybridOffsetLag(regionId);
    }

    public int getWriteComputeErrorCode() {
      if (ingestionTask == null) {
        return INACTIVE_STORE_INGESTION_TASK.code;
      }
      return ingestionTask.getWriteComputeErrorCode();
    }

    /**
     * @return 1 if the leader offset lag is greater than 0 and not actively ingesting data, otherwise 0.
     */
    public double getLeaderStalledHybridIngestion() {
      if (ingestionTask == null) {
        return 0;
      }
      if (getLeaderOffsetLag() > 0 && getLeaderBytesConsumed() == 0) {
        return 1;
      } else {
        return 0;
      }
    }

    public double getReadyToServeWithRTLag() {
      if (ingestionTask == null) {
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
      return subscribePrepLatencyAvg.measure(METRIC_CONFIG, System.currentTimeMillis());
    }
    public double getSubscribePrepLatencyMax() {
      return subscribePrepLatencyMax.measure(METRIC_CONFIG, System.currentTimeMillis());
    }

    public double getSubscribeGetConsumerLatencyAvg() {
      return subscribeGetConsumerLatencyAvg.measure(METRIC_CONFIG, System.currentTimeMillis());
    }

    public double getSubscribeGetConsumerLatencyMax() {
      return subscribeGetConsumerLatencyMax.measure(METRIC_CONFIG, System.currentTimeMillis());
    }

    public double getSubscribeConsumerSubscribeLatencyAvg() {
      return subscribeConsumerSubscribeLatencyAvg.measure(METRIC_CONFIG, System.currentTimeMillis());
    }

    public double getSubscribeConsumerSubscribeLatencyMax() {
      return subscribeConsumerSubscribeLatencyMax.measure(METRIC_CONFIG, System.currentTimeMillis());
    }

    public void recordStalePartitionsWithoutIngestionTask() {
      stalePartitionsWithoutIngestionTaskSensor.record();
    }

    public void recordSubscribePrepLatency(double value) {
      subscribePrepLatencySensor.record(value);
    }

    public double getRecordsConsumed() {
      return recordsConsumedRate.measure(METRIC_CONFIG, System.currentTimeMillis());
    }

    public void recordRecordsConsumed(double value) {
      recordsConsumedSensor.record(value);
    }

    public double getBytesConsumed() {
      return bytesConsumedRate.measure(METRIC_CONFIG, System.currentTimeMillis());
    }

    public void recordBytesConsumed(double value) {
      bytesConsumedSensor.record(value);
    }

    public double getLeaderRecordsConsumed() {
      return leaderRecordsConsumedRate.measure(METRIC_CONFIG, System.currentTimeMillis());
    }

    public void recordLeaderRecordsConsumed(double value) {
      leaderRecordsConsumedSensor.record(value);
    }

    public double getLeaderBytesConsumed() {
      return leaderBytesConsumedRate.measure(METRIC_CONFIG, System.currentTimeMillis());
    }

    public void recordLeaderBytesConsumed(double value) {
      leaderBytesConsumedSensor.record(value);
    }

    public double getFollowerRecordsConsumed() {
      return followerRecordsConsumedRate.measure(METRIC_CONFIG, System.currentTimeMillis());
    }

    public void recordFollowerRecordsConsumed(double value) {
      followerRecordsConsumedSensor.record(value);
    }

    public double getFollowerBytesConsumed() {
      return followerBytesConsumedRate.measure(METRIC_CONFIG, System.currentTimeMillis());
    }

    public void recordFollowerBytesConsumed(double value) {
      followerBytesConsumedSensor.record(value);
    }

    private void recordUpdateIgnoredDCR() {
      conflictResolutionUpdateIgnoredSensor.record();
    }

    private void recordTotalDCR() {
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

    public void recordRegionHybridBytesConsumed(int regionId, double value) {
      Sensor sensor = regionIdToHybridBytesConsumedSensorMap.get(regionId);
      if (sensor != null) {
        sensor.record(value);
      }
    }

    public double getRegionHybridRecordsConsumed(int regionId) {
      Rate rate = regionIdToHybridRecordsConsumedRateMap.get(regionId);
      return rate != null ? rate.measure(METRIC_CONFIG, System.currentTimeMillis()) : 0.0;
    }

    public void recordRegionHybridRecordsConsumed(int regionId, double value) {
      Sensor sensor = regionIdToHybridRecordsConsumedSensorMap.get(regionId);
      if (sensor != null) {
        sensor.record(value);
      }
    }

    public double getRegionHybridAvgConsumedOffset(int regionId) {
      Avg avg = regionIdToHybridAvgConsumedOffsetMap.get(regionId);
      return avg != null ? avg.measure(METRIC_CONFIG, System.currentTimeMillis()) : 0.0;
    }

    public void recordRegionHybridAvgConsumedOffset(int regionId, double value) {
      Sensor sensor = regionIdToHybridAvgConsumedOffsetSensorMap.get(regionId);
      if (sensor != null) {
        sensor.record(value);
      }
    }

    public double getLeaderRecordsProduced() {
      return leaderRecordsProducedRate.measure(METRIC_CONFIG, System.currentTimeMillis());
    }

    public double getUpdateIgnoredRate() {
      return updatedIgnoredDCRRate.measure(METRIC_CONFIG, System.currentTimeMillis());
    }

    public double getTotalDCRRate() {
      return totalDCRRate.measure(METRIC_CONFIG, System.currentTimeMillis());
    }

    public double getTombstoneCreationDCRRate() {
      return tombstoneCreationDCRRate.measure(METRIC_CONFIG, System.currentTimeMillis());
    }

    public double getTimestampRegressionDCRRate() {
      return timestampRegressionDCRRate.measure(METRIC_CONFIG, System.currentTimeMillis());
    }

    public double getOffsetRegressionDCRRate() {
      return offsetRegressionDCRRate.measure(METRIC_CONFIG, System.currentTimeMillis());
    }

    public void recordLeaderRecordsProduced(double value) {
      leaderRecordsProducedSensor.record(value);
    }

    public double getLeaderBytesProduced() {
      return leaderBytesProducedRate.measure(METRIC_CONFIG, System.currentTimeMillis());
    }

    public void recordLeaderBytesProduced(double value) {
      leaderBytesProducedSensor.record(value);
    }

    public void setIngestionTaskPushTimeoutGauge(int value) {
      ingestionTaskPushTimeoutGauge = value;
    }

    public int getIngestionTaskPushTimeoutGauge() {
      return ingestionTaskPushTimeoutGauge;
    }

  }

  static class StorageIngestionStatsReporter extends AbstractVeniceStatsReporter<StorageIngestionStats> {
    public StorageIngestionStatsReporter(MetricsRepository metricsRepository, String storeName) {
      super(metricsRepository, storeName);
    }

    @Override
    protected void registerStats() {
      registerSensor("ingestion_task_errored_gauge", new IngestionStatsGauge(this,
          () -> (double) getStats().getIngestionTaskErroredGauge()));

      registerSensor("batch_replication_lag", new IngestionStatsGauge(this, () ->
          (double) getStats().getBatchReplicationLag(), 0));
      registerSensor("leader_offset_lag", new IngestionStatsGauge(this, () ->
          (double) getStats().getLeaderOffsetLag(), 0));
      registerSensor("batch_leader_offset_lag", new IngestionStatsGauge(this, () ->
          (double) getStats().getBatchLeaderOffsetLag(), 0));
      registerSensor("hybrid_leader_offset_lag", new IngestionStatsGauge(this, () ->
          (double) getStats().getHybridLeaderOffsetLag(), 0));
      registerSensor("follower_offset_lag", new IngestionStatsGauge(this, () ->
          (double) getStats().getFollowerOffsetLag(), 0));
      registerSensor("batch_follower_offset_lag", new IngestionStatsGauge(this, () ->
          (double) getStats().getBatchFollowerOffsetLag(), 0));
      registerSensor("hybrid_follower_offset_lag", new IngestionStatsGauge(this, () ->
          (double) getStats().getHybridFollowerOffsetLag(), 0));
      registerSensor("write_compute_operation_failure", new IngestionStatsGauge(this,
          () -> (double) getStats().getWriteComputeErrorCode()));

      registerSensor("ingestion_task_push_timeout_gauge", new IngestionStatsGauge(this,
          () -> (double) getStats().getIngestionTaskPushTimeoutGauge()));

      registerSensor(RECORDS_CONSUMED_METRIC_NAME,
          new IngestionStatsGauge(this, () -> getStats().getRecordsConsumed(), 0));
      registerSensor(BYTES_CONSUMED_METRIC_NAME,
          new IngestionStatsGauge(this, () -> getStats().getBytesConsumed(), 0));
      registerSensor(LEADER_RECORDS_CONSUMED_METRIC_NAME,
          new IngestionStatsGauge(this, () -> getStats().getLeaderRecordsConsumed(), 0));
      registerSensor(LEADER_BYTES_CONSUMED_METRIC_NAME,
          new IngestionStatsGauge(this, () -> getStats().getLeaderBytesConsumed(), 0));
      registerSensor(FOLLOWER_RECORDS_CONSUMED_METRIC_NAME,
          new IngestionStatsGauge(this, () -> getStats().getFollowerRecordsConsumed(), 0));
      registerSensor(FOLLOWER_BYTES_CONSUMED_METRIC_NAME,
          new IngestionStatsGauge(this, () -> getStats().getFollowerBytesConsumed(), 0));
      registerSensor(LEADER_RECORDS_PRODUCED_METRIC_NAME,
          new IngestionStatsGauge(this, () -> getStats().getLeaderRecordsProduced(), 0));
      registerSensor(LEADER_BYTES_PRODUCED_METRIC_NAME,
          new IngestionStatsGauge(this, () -> getStats().getLeaderBytesProduced(), 0));
      registerSensor(STALE_PARTITIONS_WITHOUT_INGESTION_TASK_METRIC_NAME,
          new IngestionStatsGauge(this, () -> getStats().getStalePartitionsWithoutIngestionTaskCount(), 0));
      registerSensor(SUBSCRIBE_ACTION_PREP_LATENCY + AVG,
          new IngestionStatsGauge(this, () -> getStats().getSubscribePrepLatencyAvg(), 0));
      registerSensor(SUBSCRIBE_ACTION_PREP_LATENCY + MAX,
          new IngestionStatsGauge(this, () -> getStats().getSubscribePrepLatencyMax(), 0));
      registerSensor(SUBSCRIBE_ACTION_GET_CONSUMER_LATENCY + AVG,
          new IngestionStatsGauge(this, () -> getStats().getSubscribeGetConsumerLatencyAvg(), 0));
      registerSensor(SUBSCRIBE_ACTION_GET_CONSUMER_LATENCY + MAX,
          new IngestionStatsGauge(this, () -> getStats().getSubscribeGetConsumerLatencyMax(), 0));
      registerSensor(SUBSCRIBE_SUBSCRIBE_ACTION_CONSUMER_SUBSCRIBE_LATENCY + AVG,
          new IngestionStatsGauge(this, () -> getStats().getSubscribeConsumerSubscribeLatencyAvg(), 0));
      registerSensor(SUBSCRIBE_SUBSCRIBE_ACTION_CONSUMER_SUBSCRIBE_LATENCY + MAX,
          new IngestionStatsGauge(this, () -> getStats().getSubscribeConsumerSubscribeLatencyMax(), 0));
    }

    // Only register these stats if the store is hybrid.
    @Override
    protected void registerConditionalStats() {
      registerSensor("rt_topic_offset_lag", new IngestionStatsGauge(this, () ->
          (double) getStats().getRtTopicOffsetLag(), 0));

      registerSensor("rt_topic_offset_lag_over_threshold", new IngestionStatsGauge(this, () ->
          (double) getStats().getRtTopicOffsetLagOverThreshold(), 0));

      registerSensor("number_of_partitions_not_receive_SOBR", new IngestionStatsGauge(this, () ->
          (double) getStats().getNumberOfPartitionsNotReceiveSOBR(), 0));

      registerSensor(LEADER_STALLED_HYBRID_INGESTION_METRIC_NAME,
          new IngestionStatsGauge(this, () -> getStats().getLeaderStalledHybridIngestion(), 0));

      registerSensor(READY_TO_SERVE_WITH_RT_LAG_METRIC_NAME,
          new IngestionStatsGauge(this, () -> getStats().getReadyToServeWithRTLag(), 0));


      if (getStats().ingestionTask.isActiveActiveReplicationEnabled()) {
        registerSensor(UPDATE_IGNORED_DCR, new IngestionStatsGauge(this, () -> getStats().getUpdateIgnoredRate(), 0));
        registerSensor(TOTAL_DCR, new IngestionStatsGauge(this, () -> getStats().getTotalDCRRate(), 0));
        registerSensor(TOMBSTONE_CREATION_DCR, new IngestionStatsGauge(this, () -> getStats().getTombstoneCreationDCRRate(), 0));
        registerSensor(TIMESTAMP_REGRESSION_DCR_ERROR, new IngestionStatsGauge(this, () -> getStats().getTimestampRegressionDCRRate(), 0));
        registerSensor(OFFSET_REGRESSION_DCR_ERROR, new IngestionStatsGauge(this, () -> getStats().getOffsetRegressionDCRRate(), 0));

        for (Map.Entry<Integer, String> entry : getStats().ingestionTask.getServerConfig().getKafkaClusterIdToAliasMap().entrySet()) {
          String regionNamePrefix = RegionUtils.getRegionSpecificMetricPrefix(getStats().ingestionTask.getServerConfig().getRegionName(), entry.getValue());
          registerSensor(regionNamePrefix + "_rt_lag",
              new IngestionStatsGauge(this, () -> (double) getStats().getRegionHybridOffsetLag(entry.getKey()), 0));
          registerSensor(regionNamePrefix + "_rt_bytes_consumed",
              new IngestionStatsGauge(this, () -> getStats().getRegionHybridBytesConsumed(entry.getKey()), 0));
          registerSensor(regionNamePrefix + "_rt_records_consumed",
              new IngestionStatsGauge(this, () -> getStats().getRegionHybridRecordsConsumed(entry.getKey()), 0));
          registerSensor(regionNamePrefix + "_rt_consumed_offset",
              new IngestionStatsGauge(this, () -> getStats().getRegionHybridAvgConsumedOffset(entry.getKey()), 0));
        }
      }
    }

    private static class IngestionStatsGauge extends Gauge {
      IngestionStatsGauge(AbstractVeniceStatsReporter reporter, DoubleSupplier supplier) {
        this(reporter, supplier, NULL_INGESTION_STATS.code);
      }

      IngestionStatsGauge(AbstractVeniceStatsReporter reporter, DoubleSupplier supplier, int defaultValue) {
        /**
         * If a version doesn't exist, the corresponding reporter stat doesn't exist after the host restarts,
         * which is not an error. The users of the stats should decide whether it's reasonable to emit an error
         * code simply because the version is not created yet.
         */
        super(() -> reporter.getStats() == null ? defaultValue : supplier.getAsDouble());
      }
    }
  }
}
