package com.linkedin.davinci.stats;

import com.linkedin.davinci.kafka.consumer.StoreIngestionTask;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.stats.AbstractVeniceAggVersionedStats;
import com.linkedin.venice.stats.AbstractVeniceStatsReporter;
import com.linkedin.venice.stats.Gauge;
import com.linkedin.venice.utils.Utils;
import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Rate;
import java.util.function.Supplier;
import org.apache.log4j.Logger;

import static com.linkedin.davinci.stats.StatsErrorCode.*;


/**
 * The store level stats or the total stats will be unpopulated because there is no easy and reliable way to aggregate
 * gauge stats such as rt topic offset lag.
 */
public class AggVersionedStorageIngestionStats extends AbstractVeniceAggVersionedStats<
    AggVersionedStorageIngestionStats.StorageIngestionStats,
    AggVersionedStorageIngestionStats.StorageIngestionStatsReporter> {
  private static final Logger LOGGER = Logger.getLogger(AggVersionedStorageIngestionStats.class);

  private static final String RECORDS_CONSUMED_METRIC_NAME = "records_consumed";
  private static final String BYTES_CONSUMED_METRIC_NAME = "bytes_consumed";

  public AggVersionedStorageIngestionStats(MetricsRepository metricsRepository, ReadOnlyStoreRepository storeRepository) {
    super(metricsRepository, storeRepository, StorageIngestionStats::new, StorageIngestionStatsReporter::new);
  }

  public void setIngestionTask(String storeVersionTopic, StoreIngestionTask ingestionTask) {
    if (!Version.isVersionTopicOrStreamReprocessingTopic(storeVersionTopic)) {
      LOGGER.warn("Invalid store version topic name: " + storeVersionTopic);
      return;
    }
    VeniceSystemStoreType systemStoreType =
        VeniceSystemStoreUtils.getSystemStoreType(Version.parseStoreFromKafkaTopicName(storeVersionTopic));
    if (systemStoreType != null && systemStoreType.isStoreZkShared()) {
      // TODO This is only a temporary solution to funnel the stats to the right versions (current, backup, future).
      // Once multi-version support is available the stats version info should be updated by the handleStoreChanged of
      // the corresponding Venice store instead.
      Store zkSharedStore = ingestionTask.getIngestionStore();
      updateStatsVersionInfo(Version.parseStoreFromKafkaTopicName(storeVersionTopic), zkSharedStore.getVersions(),
          zkSharedStore.getCurrentVersion());
    }
    String storeName = Version.parseStoreFromKafkaTopicName(storeVersionTopic);
    int version = Version.parseVersionFromKafkaTopicName(storeVersionTopic);
    try {
      // Make sure the hybrid store stats are registered
      if (ingestionTask.isHybridMode()) {
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

  public void recordRecordsConsumed(String storeName, int version, int count) {
    Utils.computeIfNotNull(getTotalStats(storeName), stat -> stat.recordRecordsConsumed(count));
    Utils.computeIfNotNull(getStats(storeName, version), stat -> stat.recordRecordsConsumed(count));
  }

  public void recordBytesConsumed(String storeName, int version, long bytes) {
    Utils.computeIfNotNull(getTotalStats(storeName), stat -> stat.recordBytesConsumed(bytes));
    Utils.computeIfNotNull(getStats(storeName, version), stat -> stat.recordBytesConsumed(bytes));
  }

  static class StorageIngestionStats {
    private static final MetricConfig METRIC_CONFIG = new MetricConfig();
    private final MetricsRepository localMetricRepository = new MetricsRepository(METRIC_CONFIG);

    private StoreIngestionTask ingestionTask;
    private long rtTopicOffsetLagOverThreshold = INACTIVE_STORE_INGESTION_TASK.code;
    private int ingestionTaskErroredGauge = 0;

    private final Rate recordsConsumedRate;
    private final Rate bytesConsumedRate;

    private final Sensor recordsConsumedSensor;
    private final Sensor bytesConsumedSensor;

    public StorageIngestionStats()  {
      recordsConsumedRate = new Rate();
      recordsConsumedSensor = localMetricRepository.sensor(RECORDS_CONSUMED_METRIC_NAME);
      recordsConsumedSensor.add(RECORDS_CONSUMED_METRIC_NAME + recordsConsumedRate.getClass().getSimpleName(), recordsConsumedRate);

      bytesConsumedRate = new Rate();
      bytesConsumedSensor = localMetricRepository.sensor(BYTES_CONSUMED_METRIC_NAME);
      bytesConsumedSensor.add(BYTES_CONSUMED_METRIC_NAME + bytesConsumedRate.getClass().getSimpleName(), bytesConsumedRate);
    }

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

    public long getFollowerOffsetLag() {
      if (ingestionTask == null) {
        return INACTIVE_STORE_INGESTION_TASK.code;
      }
      return ingestionTask.getFollowerOffsetLag();
    }

    public int getWriteComputeErrorCode() {
      if (ingestionTask == null) {
        return INACTIVE_STORE_INGESTION_TASK.code;
      }
      return ingestionTask.getWriteComputeErrorCode();
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
  }

  static class StorageIngestionStatsReporter extends AbstractVeniceStatsReporter<StorageIngestionStats> {
    public StorageIngestionStatsReporter(MetricsRepository metricsRepository, String storeName) {
      super(metricsRepository, storeName);
    }

    @Override
    protected void registerStats() {
      registerSensor("ingestion_task_errored_gauge", new IngestionStatsGauge(this,
          () -> (double) getStats().getIngestionTaskErroredGauge()));

      registerSensor("follower_offset_lag", new IngestionStatsGauge(this, () ->
          (double) getStats().getFollowerOffsetLag()));
      registerSensor("write_compute_operation_failure", new IngestionStatsGauge(this,
          () -> (double) getStats().getWriteComputeErrorCode()));

      registerSensor(RECORDS_CONSUMED_METRIC_NAME,
          new IngestionStatsGauge(this, () -> getStats().getRecordsConsumed()));
      registerSensor(BYTES_CONSUMED_METRIC_NAME,
          new IngestionStatsGauge(this, () -> getStats().getBytesConsumed()));
    }

    // Only register these stats if the store is hybrid.
    @Override
    protected void registerConditionalStats() {
      registerSensor("rt_topic_offset_lag", new IngestionStatsGauge(this, () ->
          (double) getStats().getRtTopicOffsetLag()));

      registerSensor("rt_topic_offset_lag_over_threshold", new IngestionStatsGauge(this, () ->
          (double) getStats().getRtTopicOffsetLagOverThreshold()));

      registerSensor("number_of_partitions_not_receive_SOBR", new IngestionStatsGauge(this, () ->
          (double) getStats().getNumberOfPartitionsNotReceiveSOBR()));
    }

    private static class IngestionStatsGauge extends Gauge {
      IngestionStatsGauge(AbstractVeniceStatsReporter reporter, Supplier<Double> supplier) {
        super(() -> reporter.getStats() == null ? NULL_INGESTION_STATS.code : supplier.get());
      }
    }
  }
}
