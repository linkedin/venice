package com.linkedin.davinci.stats;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.kafka.consumer.StoreIngestionTask;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Version;
import io.tehuti.metrics.MetricsRepository;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * The store level stats or the total stats will be unpopulated because there is no easy and reliable way to aggregate
 * gauge stats such as rt topic offset lag.
 */
public class AggVersionedIngestionStats
    extends AbstractVeniceAggVersionedStats<IngestionStats, IngestionStatsReporter> {
  private static final Logger LOGGER = LogManager.getLogger(AggVersionedIngestionStats.class);

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
  }

  public void setIngestionTask(String storeVersionTopic, StoreIngestionTask ingestionTask) {
    if (!Version.isVersionTopicOrStreamReprocessingTopic(storeVersionTopic)) {
      LOGGER.warn("Invalid store version topic name: {}", storeVersionTopic);
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
      LOGGER.warn("Failed to set up versioned storage ingestion stats of store: {}, version: {}", storeName, version);
    }
  }

  public void recordRecordsConsumed(String storeName, int version) {
    recordVersionedAndTotalStat(storeName, version, IngestionStats::recordRecordsConsumed);
  }

  public void recordBytesConsumed(String storeName, int version, long bytes) {
    recordVersionedAndTotalStat(storeName, version, stat -> stat.recordBytesConsumed(bytes));
  }

  public void recordLeaderConsumed(String storeName, int version, long bytes) {
    recordVersionedAndTotalStat(storeName, version, stat -> {
      stat.recordLeaderBytesConsumed(bytes);
      stat.recordLeaderRecordsConsumed();
    });
  }

  public void recordFollowerConsumed(String storeName, int version, long bytes) {
    recordVersionedAndTotalStat(storeName, version, stat -> {
      stat.recordFollowerBytesConsumed(bytes);
      stat.recordFollowerRecordsConsumed();
    });
  }

  public void recordLeaderProduced(String storeName, int version, long bytesProduced, int recordCount) {
    recordVersionedAndTotalStat(storeName, version, stat -> {
      stat.recordLeaderBytesProduced(bytesProduced);
      stat.recordLeaderRecordsProduced(recordCount);
    });
  }

  public void recordRegionHybridConsumption(
      String storeName,
      int version,
      int regionId,
      long bytesConsumed,
      long offsetConsumed,
      long currentTimeMs) {
    recordVersionedAndTotalStat(storeName, version, stat -> {
      stat.recordRegionHybridBytesConsumed(regionId, bytesConsumed, currentTimeMs);
      stat.recordRegionHybridRecordsConsumed(regionId, 1, currentTimeMs);
      stat.recordRegionHybridAvgConsumedOffset(regionId, offsetConsumed, currentTimeMs);
    });
  }

  public void recordUpdateIgnoredDCR(String storeName, int version) {
    recordVersionedAndTotalStat(storeName, version, IngestionStats::recordUpdateIgnoredDCR);
  }

  public void recordTotalDCR(String storeName, int version) {
    recordVersionedAndTotalStat(storeName, version, IngestionStats::recordTotalDCR);
  }

  public void recordTimestampRegressionDCRError(String storeName, int version) {
    recordVersionedAndTotalStat(storeName, version, IngestionStats::recordTimestampRegressionDCRError);
  }

  public void recordOffsetRegressionDCRError(String storeName, int version) {
    recordVersionedAndTotalStat(storeName, version, IngestionStats::recordOffsetRegressionDCRError);
  }

  public void recordTombStoneCreationDCR(String storeName, int version) {
    recordVersionedAndTotalStat(storeName, version, IngestionStats::recordTombStoneCreationDCR);
  }

  public void setIngestionTaskPushTimeoutGauge(String storeName, int version) {
    getStats(storeName, version).setIngestionTaskPushTimeoutGauge(1);
  }

  public void resetIngestionTaskPushTimeoutGauge(String storeName, int version) {
    getStats(storeName, version).setIngestionTaskPushTimeoutGauge(0);
  }

  public void recordStalePartitionsWithoutIngestionTask(String storeName, int version) {
    recordVersionedAndTotalStat(storeName, version, IngestionStats::recordStalePartitionsWithoutIngestionTask);
  }

  public void recordSubscribePrepLatency(String storeName, int version, double value) {
    long currentTimeMs = System.currentTimeMillis();
    recordVersionedAndTotalStat(storeName, version, stat -> stat.recordSubscribePrepLatency(value, currentTimeMs));
  }

  public void recordConsumedRecordEndToEndProcessingLatency(
      String storeName,
      int version,
      double value,
      long currentTimeMs) {
    recordVersionedAndTotalStat(
        storeName,
        version,
        stat -> stat.recordConsumedRecordEndToEndProcessingLatency(value, currentTimeMs));
  }

  public void recordVersionTopicEndOffsetRewind(String storeName, int version) {
    recordVersionedAndTotalStat(storeName, version, IngestionStats::recordVersionTopicEndOffsetRewind);
  }

  public void recordNearlineProducerToLocalBrokerLatency(String storeName, int version, double value, long timestamp) {
    recordVersionedAndTotalStat(
        storeName,
        version,
        stat -> stat.recordNearlineProducerToLocalBrokerLatency(value, timestamp));
  }

  public void recordNearlineLocalBrokerToReadyToServeLatency(
      String storeName,
      int version,
      double value,
      long timestamp) {
    recordVersionedAndTotalStat(
        storeName,
        version,
        stat -> stat.recordNearlineLocalBrokerToReadyToServeLatency(value, timestamp));
  }
}
