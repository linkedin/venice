package com.linkedin.venice.stats;

import com.linkedin.venice.kafka.consumer.StoreIngestionTask;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Version;
import io.tehuti.metrics.MetricsRepository;
import java.util.function.Supplier;
import org.apache.log4j.Logger;

import static com.linkedin.venice.stats.StatsErrorCode.*;


/**
 * The store level stats or the total stats will be unpopulated because there is no easy and reliable way to aggregate
 * gauge stats such as rt topic offset lag.
 */
public class AggVersionedStorageIngestionStats extends AbstractVeniceAggVersionedStats<
    AggVersionedStorageIngestionStats.StorageIngestionStats,
    AggVersionedStorageIngestionStats.StorageIngestionStatsReporter> {
  private static final Logger LOGGER = Logger.getLogger(AggVersionedStorageIngestionStats.class);

  public AggVersionedStorageIngestionStats(MetricsRepository metricsRepository, ReadOnlyStoreRepository storeRepository) {
    super(metricsRepository, storeRepository, StorageIngestionStats::new, StorageIngestionStatsReporter::new);
  }

  public void setIngestionTask(String storeVersionTopic, StoreIngestionTask ingestionTask) {
    if (!Version.isVersionTopicOrStreamReprocessingTopic(storeVersionTopic)) {
      LOGGER.warn("Invalid store version topic name: " + storeVersionTopic);
      return;
    }
    String storeName = Version.parseStoreFromKafkaTopicName(storeVersionTopic);
    int version = Version.parseVersionFromKafkaTopicName(storeVersionTopic);
    try {
      if (ingestionTask.isHybridMode()) {
        // Make sure the hybrid store stats are registered
        registerConditionalStats(storeName);
      }
      getStats(storeName, version).setIngestionTask(ingestionTask);
    } catch (Exception e) {
      LOGGER.warn("Failed to set up versioned storage ingestion stats of store: " + storeName
          + ", version: " + version);
    }
  }

  // To prevent this metric being too noisy and align with the PreNotificationCheck of reportError, this flag should
  // only be set if the ingestion task errored after EOP is received for any of the partitions.
  public void setIngestionTaskErroredGauge(String storeName, int version) {
    getStats(storeName, version).setIngestionTaskErroredGauge(1);
  }

  public void resetIngestionTaskErroredGauge(String storeName, int version) {
    getStats(storeName, version).setIngestionTaskErroredGauge(0);
  }

  static class StorageIngestionStats {
    private StoreIngestionTask ingestionTask;
    private long rtTopicOffsetLagOverThreshold = INACTIVE_STORE_INGESTION_TASK.code;
    private int ingestionTaskErroredGauge = 0;

    public void setIngestionTask(StoreIngestionTask ingestionTask) { this.ingestionTask = ingestionTask; }

    public long getRtTopicOffsetLag() {
      if (ingestionTask == null) {
        return INACTIVE_STORE_INGESTION_TASK.code;
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

    public void setIngestionTaskErroredGauge(int value) {
      ingestionTaskErroredGauge = value;
    }

    public int getIngestionTaskErroredGauge() {
      return ingestionTaskErroredGauge;
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
    }

    // Only register these stats if the store is hybrid.
    @Override
    protected void registerConditionalStats() {
      registerSensor("rt_topic_offset_lag", new IngestionStatsGauge(this,
          () -> (double) getStats().getRtTopicOffsetLag()));

      registerSensor("rt_topic_offset_lag_over_threshold", new IngestionStatsGauge(this,
          () -> (double) getStats().getRtTopicOffsetLagOverThreshold()));

      registerSensor("number_of_partitions_not_receive_SOBR", new IngestionStatsGauge(this,
          () -> (double) getStats().getNumberOfPartitionsNotReceiveSOBR()));
    }

    private static class IngestionStatsGauge extends Gauge {
      IngestionStatsGauge(AbstractVeniceStatsReporter reporter, Supplier<Double> supplier) {
        super(() -> reporter.getStats() == null ? NULL_INGESTION_STATS.code : supplier.get());
      }
    }
  }
}
