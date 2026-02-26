package com.linkedin.davinci.stats;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.kafka.consumer.StoreIngestionTask;
import com.linkedin.davinci.stats.ingestion.IngestionOtelStats;
import com.linkedin.davinci.stats.ingestion.NoOpIngestionOtelStats;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.stats.dimensions.ReplicaType;
import com.linkedin.venice.stats.dimensions.VeniceDCREvent;
import com.linkedin.venice.stats.dimensions.VeniceDCROperation;
import com.linkedin.venice.stats.dimensions.VeniceIngestionDestinationComponent;
import com.linkedin.venice.stats.dimensions.VeniceIngestionFailureReason;
import com.linkedin.venice.stats.dimensions.VeniceIngestionSourceComponent;
import com.linkedin.venice.stats.dimensions.VeniceRecordType;
import com.linkedin.venice.stats.dimensions.VeniceRegionLocality;
import com.linkedin.venice.stats.dimensions.VeniceWriteComputeOperation;
import com.linkedin.venice.utils.RegionUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.views.VeniceView;
import io.tehuti.metrics.MetricsRepository;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * The store level stats or the total stats will be unpopulated because there is no easy and reliable way to aggregate
 * gauge stats such as rt topic offset lag.
 *
 * <p><b>OTel stats lifecycle:</b> OTel stats are created lazily by {@link #getIngestionOtelStats} and
 * updated by {@link #onVersionInfoUpdated} via {@code computeIfPresent}. This class uses eager loading
 * ({@code loadAllStats()} is NOT overridden), so {@code onVersionInfoUpdated} and
 * {@code cleanupVersionResources} need null guards because they are called during the super()
 * constructor before subclass fields ({@code otelStatsMap}) are initialized.
 */
public class AggVersionedIngestionStats
    extends AbstractVeniceAggVersionedStats<IngestionStats, IngestionStatsReporter> {
  private static final Logger LOGGER = LogManager.getLogger(AggVersionedIngestionStats.class);

  private final Map<String, IngestionOtelStats> otelStatsMap = new VeniceConcurrentHashMap<>();
  private final String clusterName;
  private final String localRegionName;
  private final boolean emitOtelIngestionStats;

  public AggVersionedIngestionStats(
      MetricsRepository metricsRepository,
      ReadOnlyStoreRepository storeRepository,
      VeniceServerConfig serverConfig) {
    super(
        metricsRepository,
        storeRepository,
        () -> new IngestionStats(serverConfig),
        IngestionStatsReporter::new,
        serverConfig.isUnregisterMetricForDeletedStoreEnabled());
    this.clusterName = serverConfig.getClusterName();
    this.localRegionName = RegionUtils.normalizeRegionName(serverConfig.getRegionName());
    this.emitOtelIngestionStats = serverConfig.isIngestionOtelStatsEnabled();
  }

  /** Updates version info for existing OTel stats only. Null guard needed because eager loading
   *  calls this from the super() constructor before {@code otelStatsMap} is initialized. */
  @Override
  protected void onVersionInfoUpdated(String storeName, int currentVersion, int futureVersion) {
    if (otelStatsMap == null) {
      return; // Called during super() constructor before otelStatsMap is initialized
    }
    otelStatsMap.computeIfPresent(storeName, (k, stats) -> {
      stats.updateVersionInfo(currentVersion, futureVersion);
      return stats;
    });
  }

  @Override
  protected void cleanupVersionResources(String storeName, int version) {
    if (otelStatsMap == null) {
      return; // Called during super() constructor before otelStatsMap is initialized
    }
    otelStatsMap.computeIfPresent(storeName, (k, stats) -> {
      stats.removeIngestionTask(version);
      return stats;
    });
  }

  @Override
  public void handleStoreDeleted(String storeName) {
    try {
      super.handleStoreDeleted(storeName);
    } finally {
      IngestionOtelStats otelStats = otelStatsMap.remove(storeName);
      if (otelStats != null) {
        otelStats.close();
      }
    }
  }

  /**
   * Gets or creates OTel stats for a store. {@code getCurrentVersion}/{@code getFutureVersion}
   * are called <b>before</b> {@code computeIfAbsent} because they can trigger
   * {@code addStore} → {@code onVersionInfoUpdated} → {@code otelStatsMap.computeIfPresent},
   * which would re-enter this same map from inside the lambda (violates ConcurrentHashMap contract).
   * The {@code get()} fast-path skips these calls when stats already exist.
   */
  private IngestionOtelStats getIngestionOtelStats(String storeName) {
    if (!emitOtelIngestionStats) {
      return NoOpIngestionOtelStats.INSTANCE;
    }
    IngestionOtelStats existing = otelStatsMap.get(storeName);
    if (existing != null) {
      return existing;
    }
    int currentVersion = getCurrentVersion(storeName);
    int futureVersion = getFutureVersion(storeName);
    return otelStatsMap.computeIfAbsent(storeName, k -> {
      IngestionOtelStats stats = new IngestionOtelStats(getMetricsRepository(), k, clusterName, localRegionName);
      stats.updateVersionInfo(currentVersion, futureVersion);
      return stats;
    });
  }

  private void recordOtelConsumptionMetrics(String storeName, int version, ReplicaType replicaType, long bytes) {
    IngestionOtelStats otelStats = getIngestionOtelStats(storeName);
    otelStats.recordBytesConsumed(version, replicaType, bytes);
    otelStats.recordRecordsConsumed(version, replicaType, 1);
  }

  public void setIngestionTask(String storeVersionTopic, StoreIngestionTask ingestionTask) {
    if (!Version.isATopicThatIsVersioned(storeVersionTopic)) {
      LOGGER.warn("Invalid store version topic name: {}", storeVersionTopic);
      return;
    }
    // For metrics reporting purpose the store name for Venice view ingestion will be <storeName>_<viewName>
    String storeName = VeniceView.isViewTopic(storeVersionTopic)
        ? VeniceView.parseStoreAndViewFromViewTopic(storeVersionTopic)
        : Version.parseStoreFromKafkaTopicName(storeVersionTopic);
    int version = Version.parseVersionFromKafkaTopicName(storeVersionTopic);
    try {
      /**
       * Set up the ingestion task reference before registering any metrics that depend on the task reference.
       */
      getStats(storeName, version).setIngestionTask(ingestionTask);

      // OTel metrics - set the ingestion task reference for ASYNC_GAUGE callbacks
      getIngestionOtelStats(storeName).setIngestionTask(version, ingestionTask);

      // Make sure the hybrid store stats are registered
      if (ingestionTask.isHybridMode()) {
        registerConditionalStats(storeName);
      }
    } catch (Exception e) {
      LOGGER
          .warn("Failed to set up versioned storage ingestion stats of store: {}, version: {}", storeName, version, e);
    }
  }

  /**
   * Records generic records consumed metric (Tehuti only).
   *
   * <p>OTel metrics are intentionally NOT recorded here to avoid double-counting.
   * OTel consumption metrics include a ReplicaType dimension (LEADER/FOLLOWER) and are
   * recorded by {@link #recordLeaderConsumed} and {@link #recordFollowerConsumed} instead.
   */
  public void recordRecordsConsumed(String storeName, int version) {
    // Tehuti metrics only - OTel uses leader/follower specific methods with ReplicaType dimension
    recordVersionedAndTotalStat(storeName, version, IngestionStats::recordRecordsConsumed);
  }

  /**
   * Records generic bytes consumed metric (Tehuti only).
   *
   * <p>OTel metrics are intentionally NOT recorded here to avoid double-counting.
   * OTel consumption metrics include a ReplicaType dimension (LEADER/FOLLOWER) and are
   * recorded by {@link #recordLeaderConsumed} and {@link #recordFollowerConsumed} instead.
   */
  public void recordBytesConsumed(String storeName, int version, long bytes) {
    // Tehuti metrics only - OTel uses leader/follower specific methods with ReplicaType dimension
    recordVersionedAndTotalStat(storeName, version, stat -> stat.recordBytesConsumed(bytes));
  }

  public void recordLeaderConsumed(String storeName, int version, long bytes) {
    // Tehuti metrics
    recordVersionedAndTotalStat(storeName, version, stat -> {
      stat.recordLeaderBytesConsumed(bytes);
      stat.recordLeaderRecordsConsumed();
    });
    // OTel metrics
    recordOtelConsumptionMetrics(storeName, version, ReplicaType.LEADER, bytes);
  }

  public void recordFollowerConsumed(String storeName, int version, long bytes) {
    // Tehuti metrics
    recordVersionedAndTotalStat(storeName, version, stat -> {
      stat.recordFollowerBytesConsumed(bytes);
      stat.recordFollowerRecordsConsumed();
    });
    // OTel metrics
    recordOtelConsumptionMetrics(storeName, version, ReplicaType.FOLLOWER, bytes);
  }

  public void recordLeaderProduced(String storeName, int version, long bytesProduced, int recordCount) {
    // Tehuti metrics
    recordVersionedAndTotalStat(storeName, version, stat -> {
      stat.recordLeaderBytesProduced(bytesProduced);
      stat.recordLeaderRecordsProduced(recordCount);
    });
    // OTel metrics
    IngestionOtelStats otelStats = getIngestionOtelStats(storeName);
    otelStats.recordBytesProduced(version, ReplicaType.LEADER, bytesProduced);
    otelStats.recordRecordsProduced(version, ReplicaType.LEADER, recordCount);
  }

  public void recordRegionHybridConsumption(
      String storeName,
      int version,
      int regionId,
      long bytesConsumed,
      long currentTimeMs,
      String sourceRegion,
      VeniceRegionLocality regionLocality) {
    // Tehuti metrics
    recordVersionedAndTotalStat(storeName, version, stat -> {
      stat.recordRegionHybridBytesConsumed(regionId, bytesConsumed, currentTimeMs);
      stat.recordRegionHybridRecordsConsumed(regionId, 1, currentTimeMs);
    });
    // OTel RT metrics
    IngestionOtelStats otelStats = getIngestionOtelStats(storeName);
    otelStats.recordRtBytesConsumed(version, sourceRegion, regionLocality, bytesConsumed);
    otelStats.recordRtRecordsConsumed(version, sourceRegion, regionLocality, 1);
  }

  public void recordUpdateIgnoredDCR(String storeName, int version) {
    // Tehuti metrics
    recordVersionedAndTotalStat(storeName, version, IngestionStats::recordUpdateIgnoredDCR);
    // OTel metrics
    getIngestionOtelStats(storeName).recordDcrEventCount(version, VeniceDCREvent.UPDATE_IGNORED, 1);
  }

  public void recordTotalDCR(String storeName, int version) {
    // Tehuti metrics
    recordVersionedAndTotalStat(storeName, version, IngestionStats::recordTotalDCR);
    // OTel metrics
    getIngestionOtelStats(storeName).recordDcrTotalCount(version, 1);
  }

  public void recordTotalDuplicateKeyUpdate(String storeName, int version) {
    // Tehuti metrics
    recordVersionedAndTotalStat(storeName, version, IngestionStats::recordTotalDuplicateKeyUpdate);
    // OTel metrics
    getIngestionOtelStats(storeName).recordDuplicateKeyUpdateCount(version, 1);
  }

  public void recordTimestampRegressionDCRError(String storeName, int version) {
    // Tehuti metrics
    recordVersionedAndTotalStat(storeName, version, IngestionStats::recordTimestampRegressionDCRError);
    // OTel metrics
    getIngestionOtelStats(storeName).recordDcrEventCount(version, VeniceDCREvent.TIMESTAMP_REGRESSION_ERROR, 1);
  }

  public void recordOffsetRegressionDCRError(String storeName, int version) {
    // Tehuti metrics
    recordVersionedAndTotalStat(storeName, version, IngestionStats::recordOffsetRegressionDCRError);
    // OTel metrics
    getIngestionOtelStats(storeName).recordDcrEventCount(version, VeniceDCREvent.OFFSET_REGRESSION_ERROR, 1);
  }

  public void recordTombStoneCreationDCR(String storeName, int version) {
    // Tehuti metrics
    recordVersionedAndTotalStat(storeName, version, IngestionStats::recordTombStoneCreationDCR);
    // OTel metrics
    getIngestionOtelStats(storeName).recordDcrEventCount(version, VeniceDCREvent.TOMBSTONE_CREATION, 1);
  }

  public void setIngestionTaskPushTimeoutGauge(String storeName, int version) {
    // Tehuti metrics
    getStats(storeName, version).setIngestionTaskPushTimeoutGauge(1);
    // OTel metrics
    getIngestionOtelStats(storeName).setIngestionTaskPushTimeoutGauge(version, 1);
  }

  public void resetIngestionTaskPushTimeoutGauge(String storeName, int version) {
    // Tehuti metrics
    getStats(storeName, version).setIngestionTaskPushTimeoutGauge(0);
    // OTel metrics
    getIngestionOtelStats(storeName).setIngestionTaskPushTimeoutGauge(version, 0);
  }

  public void recordSubscribePrepLatency(String storeName, int version, double value) {
    long currentTimeMs = System.currentTimeMillis();
    // Tehuti metrics
    recordVersionedAndTotalStat(storeName, version, stat -> stat.recordSubscribePrepLatency(value, currentTimeMs));
    // OTel metrics
    getIngestionOtelStats(storeName).recordSubscribePrepTime(version, value);
  }

  public void recordProducerCallBackLatency(String storeName, int version, double value, long currentTimeMs) {
    // Tehuti metrics
    recordVersionedAndTotalStat(storeName, version, stat -> stat.recordProducerCallBackLatency(value, currentTimeMs));
    // OTel metrics - producer callback is always for leader
    getIngestionOtelStats(storeName).recordProducerCallbackTime(version, ReplicaType.LEADER, value);
  }

  public void recordLeaderPreprocessingLatency(String storeName, int version, double value, long currentTimeMs) {
    // Tehuti metrics
    recordVersionedAndTotalStat(
        storeName,
        version,
        stat -> stat.recordLeaderPreprocessingLatency(value, currentTimeMs));
    // OTel metrics
    getIngestionOtelStats(storeName).recordPreprocessingLeaderTime(version, value);
  }

  public void recordInternalPreprocessingLatency(String storeName, int version, double value, long currentTimeMs) {
    // Tehuti metrics
    recordVersionedAndTotalStat(
        storeName,
        version,
        stat -> stat.recordInternalPreprocessingLatency(value, currentTimeMs));
    // OTel metrics
    getIngestionOtelStats(storeName).recordPreprocessingInternalTime(version, value);
  }

  public void recordLeaderLatencies(
      String storeName,
      int version,
      long currentTimeMs,
      double producerBrokerLatencyMs,
      double brokerConsumerLatencyMs) {
    // Tehuti metrics
    recordVersionedAndTotalStat(storeName, version, stat -> {
      stat.recordProducerSourceBrokerLatencyMs(producerBrokerLatencyMs, currentTimeMs);
      stat.recordSourceBrokerLeaderConsumerLatencyMs(brokerConsumerLatencyMs, currentTimeMs);
    });
    // OTel metrics
    IngestionOtelStats otelStats = getIngestionOtelStats(storeName);
    otelStats.recordTimeBetweenComponents(
        version,
        VeniceIngestionSourceComponent.PRODUCER,
        VeniceIngestionDestinationComponent.SOURCE_BROKER,
        producerBrokerLatencyMs);
    otelStats.recordTimeBetweenComponents(
        version,
        VeniceIngestionSourceComponent.SOURCE_BROKER,
        VeniceIngestionDestinationComponent.LEADER_CONSUMER,
        brokerConsumerLatencyMs);
  }

  public void recordFollowerLatencies(
      String storeName,
      int version,
      long currentTimeMs,
      double producerBrokerLatencyMs,
      double brokerConsumerLatencyMs) {
    // Tehuti metrics
    recordVersionedAndTotalStat(storeName, version, stat -> {
      stat.recordProducerLocalBrokerLatencyMs(producerBrokerLatencyMs, currentTimeMs);
      stat.recordLocalBrokerFollowerConsumerLatencyMs(brokerConsumerLatencyMs, currentTimeMs);
    });
    // OTel metrics
    IngestionOtelStats otelStats = getIngestionOtelStats(storeName);
    otelStats.recordTimeBetweenComponents(
        version,
        VeniceIngestionSourceComponent.PRODUCER,
        VeniceIngestionDestinationComponent.LOCAL_BROKER,
        producerBrokerLatencyMs);
    otelStats.recordTimeBetweenComponents(
        version,
        VeniceIngestionSourceComponent.LOCAL_BROKER,
        VeniceIngestionDestinationComponent.FOLLOWER_CONSUMER,
        brokerConsumerLatencyMs);
  }

  public void recordLeaderProducerCompletionTime(String storeName, int version, double value, long currentTimeMs) {
    // Tehuti metrics
    recordVersionedAndTotalStat(
        storeName,
        version,
        stat -> stat.recordLeaderProducerCompletionLatencyMs(value, currentTimeMs));
    // OTel metrics
    getIngestionOtelStats(storeName).recordProducerTime(version, value);
  }

  public void recordConsumedRecordEndToEndProcessingLatency(
      String storeName,
      int version,
      double value,
      long currentTimeMs) {
    // Tehuti metrics
    recordVersionedAndTotalStat(
        storeName,
        version,
        stat -> stat.recordConsumedRecordEndToEndProcessingLatency(value, currentTimeMs));
    // OTel metrics
    getIngestionOtelStats(storeName).recordIngestionTime(version, value);
  }

  public void recordNearlineProducerToLocalBrokerLatency(String storeName, int version, double value, long timestamp) {
    // Tehuti metrics only
    recordVersionedAndTotalStat(
        storeName,
        version,
        stat -> stat.recordNearlineProducerToLocalBrokerLatency(value, timestamp));
  }

  public void recordMaxIdleTime(String storeName, int version, long idleTimeMs) {
    // Tehuti metrics
    getStats(storeName, version).recordIdleTime(idleTimeMs);
    // OTel metrics
    getIngestionOtelStats(storeName).recordIdleTime(version, idleTimeMs);
  }

  public void recordBatchProcessingRequest(String storeName, int version, int size, long timestamp) {
    // Tehuti metrics
    recordVersionedAndTotalStat(storeName, version, stat -> stat.recordBatchProcessingRequest(size, timestamp));
    // OTel metrics
    IngestionOtelStats otelStats = getIngestionOtelStats(storeName);
    otelStats.recordBatchProcessingRequestCount(version, 1);
    otelStats.recordBatchProcessingRequestRecordCount(version, size);
  }

  public void recordBatchProcessingRequestError(String storeName, int version) {
    // Tehuti metrics
    recordVersionedAndTotalStat(storeName, version, stat -> stat.recordBatchProcessingRequestError());
    // OTel metrics
    getIngestionOtelStats(storeName).recordBatchProcessingRequestErrorCount(version, 1);
  }

  public void recordBatchProcessingLatency(String storeName, int version, double latency, long timestamp) {
    // Tehuti metrics
    recordVersionedAndTotalStat(
        storeName,
        version,
        stat -> stat.recordBatchProcessingRequestLatency(latency, timestamp));
    // OTel metrics
    getIngestionOtelStats(storeName).recordBatchProcessingRequestTime(version, latency);
  }

  // --- HostLevelIngestionStats OTel methods (no Tehuti — Tehuti is recorded directly via hostLevelIngestionStats at
  // the call site) ---

  // Simple latency methods

  public void recordConsumerQueuePutTime(String storeName, int version, double latencyMs) {
    getIngestionOtelStats(storeName).recordConsumerQueuePutTime(version, latencyMs);
  }

  public void recordStorageEnginePutTime(String storeName, int version, double latencyMs) {
    getIngestionOtelStats(storeName).recordStorageEnginePutTime(version, latencyMs);
  }

  public void recordStorageEngineDeleteTime(String storeName, int version, double latencyMs) {
    getIngestionOtelStats(storeName).recordStorageEngineDeleteTime(version, latencyMs);
  }

  public void recordConsumerActionTime(String storeName, int version, double latencyMs) {
    getIngestionOtelStats(storeName).recordConsumerActionTime(version, latencyMs);
  }

  public void recordLongRunningTaskCheckTime(String storeName, int version, double latencyMs) {
    getIngestionOtelStats(storeName).recordLongRunningTaskCheckTime(version, latencyMs);
  }

  public void recordViewWriterProduceTime(String storeName, int version, double latencyMs) {
    getIngestionOtelStats(storeName).recordViewWriterProduceTime(version, latencyMs);
  }

  public void recordViewWriterAckTime(String storeName, int version, double latencyMs) {
    getIngestionOtelStats(storeName).recordViewWriterAckTime(version, latencyMs);
  }

  public void recordProducerEnqueueTime(String storeName, int version, double latencyMs) {
    getIngestionOtelStats(storeName).recordProducerEnqueueTime(version, latencyMs);
  }

  public void recordProducerCompressTime(String storeName, int version, double latencyMs) {
    getIngestionOtelStats(storeName).recordProducerCompressTime(version, latencyMs);
  }

  public void recordProducerSynchronizeTime(String storeName, int version, double latencyMs) {
    getIngestionOtelStats(storeName).recordProducerSynchronizeTime(version, latencyMs);
  }

  // Latency methods with 2nd enum dimension

  public void recordWriteComputeTime(String storeName, int version, VeniceWriteComputeOperation op, double latencyMs) {
    getIngestionOtelStats(storeName).recordWriteComputeTime(version, op, latencyMs);
  }

  public void recordDcrLookupTime(String storeName, int version, VeniceRecordType recordType, double latencyMs) {
    getIngestionOtelStats(storeName).recordDcrLookupTime(version, recordType, latencyMs);
  }

  public void recordDcrMergeTime(String storeName, int version, VeniceDCROperation op, double latencyMs) {
    getIngestionOtelStats(storeName).recordDcrMergeTime(version, op, latencyMs);
  }

  // Simple count methods

  public void recordUnexpectedMessageCount(String storeName, int version) {
    getIngestionOtelStats(storeName).recordUnexpectedMessageCount(version, 1);
  }

  public void recordStoreMetadataInconsistentCount(String storeName, int version) {
    getIngestionOtelStats(storeName).recordStoreMetadataInconsistentCount(version, 1);
  }

  public void recordResubscriptionFailureCount(String storeName, int version) {
    getIngestionOtelStats(storeName).recordResubscriptionFailureCount(version, 1);
  }

  public void recordWriteComputeCacheHitCount(String storeName, int version) {
    getIngestionOtelStats(storeName).recordWriteComputeCacheHitCount(version, 1);
  }

  public void recordChecksumVerificationFailureCount(String storeName, int version) {
    getIngestionOtelStats(storeName).recordChecksumVerificationFailureCount(version, 1);
  }

  // Count methods with 2nd enum dimension

  public void recordIngestionFailureCount(String storeName, int version, VeniceIngestionFailureReason reason) {
    getIngestionOtelStats(storeName).recordIngestionFailureCount(version, reason, 1);
  }

  public void recordDcrLookupCacheHitCount(String storeName, int version, VeniceRecordType recordType) {
    getIngestionOtelStats(storeName).recordDcrLookupCacheHitCount(version, recordType, 1);
  }

  // Size/rate methods

  public void recordBytesConsumedAsUncompressedSize(String storeName, int version, long bytes) {
    getIngestionOtelStats(storeName).recordBytesConsumedAsUncompressedSize(version, bytes);
  }

  public void recordKeySize(String storeName, int version, long bytes) {
    getIngestionOtelStats(storeName).recordKeySize(version, bytes);
  }

  public void recordValueSize(String storeName, int version, long bytes) {
    getIngestionOtelStats(storeName).recordValueSize(version, bytes);
  }

  public void recordAssembledSize(String storeName, int version, VeniceRecordType recordType, long bytes) {
    getIngestionOtelStats(storeName).recordAssembledSize(version, recordType, bytes);
  }

  public void recordAssembledSizeRatio(String storeName, int version, double ratio) {
    getIngestionOtelStats(storeName).recordAssembledSizeRatio(version, ratio);
  }
}
