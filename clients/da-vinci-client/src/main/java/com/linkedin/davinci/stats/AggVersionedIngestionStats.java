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
      LOGGER
          .warn("Failed to set up versioned storage ingestion stats of store: {}, version: {}", storeName, version, e);
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

  public void recordSubscribePrepLatency(String storeName, int version, double value) {
    long currentTimeMs = System.currentTimeMillis();
    recordVersionedAndTotalStat(storeName, version, stat -> stat.recordSubscribePrepLatency(value, currentTimeMs));
  }

  public void recordProducerCallBackLatency(String storeName, int version, double value, long currentTimeMs) {
    recordVersionedAndTotalStat(storeName, version, stat -> stat.recordProducerCallBackLatency(value, currentTimeMs));
  }

  public void recordLeaderPreprocessingLatency(String storeName, int version, double value, long currentTimeMs) {
    recordVersionedAndTotalStat(
        storeName,
        version,
        stat -> stat.recordLeaderPreprocessingLatency(value, currentTimeMs));
  }

  public void recordInternalPreprocessingLatency(String storeName, int version, double value, long currentTimeMs) {
    recordVersionedAndTotalStat(
        storeName,
        version,
        stat -> stat.recordInternalPreprocessingLatency(value, currentTimeMs));
  }

  public void recordLeaderLatencies(
      String storeName,
      int version,
      long currentTimeMs,
      double producerBrokerLatencyMs,
      double brokerConsumerLatencyMs) {
    recordVersionedAndTotalStat(storeName, version, stat -> {
      stat.recordProducerSourceBrokerLatencyMs(producerBrokerLatencyMs, currentTimeMs);
      stat.recordSourceBrokerLeaderConsumerLatencyMs(brokerConsumerLatencyMs, currentTimeMs);
    });
  }

  public void recordFollowerLatencies(
      String storeName,
      int version,
      long currentTimeMs,
      double producerBrokerLatencyMs,
      double brokerConsumerLatencyMs) {
    recordVersionedAndTotalStat(storeName, version, stat -> {
      stat.recordProducerLocalBrokerLatencyMs(producerBrokerLatencyMs, currentTimeMs);
      stat.recordLocalBrokerFollowerConsumerLatencyMs(brokerConsumerLatencyMs, currentTimeMs);
    });
  }

  public void recordLeaderProducerCompletionTime(String storeName, int version, double value, long currentTimeMs) {
    recordVersionedAndTotalStat(
        storeName,
        version,
        stat -> stat.recordLeaderProducerCompletionLatencyMs(value, currentTimeMs));
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

  public void recordTransformerLatency(String storeName, int version, double value, long timestamp) {
    recordVersionedAndTotalStat(storeName, version, stat -> stat.recordTransformerLatency(value, timestamp));
  }

  public void recordTransformerLifecycleStartLatency(String storeName, int version, double value, long timestamp) {
    recordVersionedAndTotalStat(
        storeName,
        version,
        stat -> stat.recordTransformerLifecycleStartLatency(value, timestamp));
  }

  public void recordTransformerLifecycleEndLatency(String storeName, int version, double value, long timestamp) {
    recordVersionedAndTotalStat(
        storeName,
        version,
        stat -> stat.recordTransformerLifecycleEndLatency(value, timestamp));
  }

  public void recordTransformerError(String storeName, int version, double value, long timestamp) {
    recordVersionedAndTotalStat(storeName, version, stat -> stat.recordTransformerError(value, timestamp));
  }

  public void recordMaxIdleTime(String storeName, int version, long idleTimeMs) {
    getStats(storeName, version).recordIdleTime(idleTimeMs);
  }

  public void registerTransformerLatencySensor(String storeName, int version) {
    getStats(storeName, version).registerTransformerLatencySensor();
    getTotalStats(storeName).registerTransformerLatencySensor();
  }

  public void registerTransformerLifecycleStartLatency(String storeName, int version) {
    getStats(storeName, version).registerTransformerLifecycleStartLatencySensor();
    getTotalStats(storeName).registerTransformerLifecycleStartLatencySensor();
  }

  public void registerTransformerLifecycleEndLatency(String storeName, int version) {
    getStats(storeName, version).registerTransformerLifecycleEndLatencySensor();
    getTotalStats(storeName).registerTransformerLifecycleEndLatencySensor();
  }

  public void registerTransformerErrorSensor(String storeName, int version) {
    getStats(storeName, version).registerTransformerErrorSensor();
    getTotalStats(storeName).registerTransformerErrorSensor();
  }

  public void recordBatchProcessingRequest(String storeName, int version, int size, long timestamp) {
    recordVersionedAndTotalStat(storeName, version, stat -> stat.recordBatchProcessingRequest(size, timestamp));
  }

  public void recordBatchProcessingRequestError(String storeName, int version) {
    recordVersionedAndTotalStat(storeName, version, stat -> stat.recordBatchProcessingRequestError());
  }

  public void recordBatchProcessingLatency(String storeName, int version, double latency, long timestamp) {
    recordVersionedAndTotalStat(
        storeName,
        version,
        stat -> stat.recordBatchProcessingRequestLatency(latency, timestamp));
  }
}
