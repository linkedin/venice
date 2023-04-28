package com.linkedin.davinci.stats;

import static com.linkedin.davinci.stats.IngestionStats.BATCH_FOLLOWER_OFFSET_LAG;
import static com.linkedin.davinci.stats.IngestionStats.BATCH_LEADER_OFFSET_LAG;
import static com.linkedin.davinci.stats.IngestionStats.BATCH_REPLICATION_LAG;
import static com.linkedin.davinci.stats.IngestionStats.BYTES_CONSUMED_METRIC_NAME;
import static com.linkedin.davinci.stats.IngestionStats.CONSUMED_RECORD_END_TO_END_PROCESSING_LATENCY;
import static com.linkedin.davinci.stats.IngestionStats.FOLLOWER_BYTES_CONSUMED_METRIC_NAME;
import static com.linkedin.davinci.stats.IngestionStats.FOLLOWER_OFFSET_LAG;
import static com.linkedin.davinci.stats.IngestionStats.FOLLOWER_RECORDS_CONSUMED_METRIC_NAME;
import static com.linkedin.davinci.stats.IngestionStats.HYBRID_FOLLOWER_OFFSET_LAG;
import static com.linkedin.davinci.stats.IngestionStats.HYBRID_LEADER_OFFSET_LAG;
import static com.linkedin.davinci.stats.IngestionStats.INGESTION_TASK_ERROR_GAUGE;
import static com.linkedin.davinci.stats.IngestionStats.INGESTION_TASK_PUSH_TIMEOUT_GAUGE;
import static com.linkedin.davinci.stats.IngestionStats.LEADER_BYTES_CONSUMED_METRIC_NAME;
import static com.linkedin.davinci.stats.IngestionStats.LEADER_BYTES_PRODUCED_METRIC_NAME;
import static com.linkedin.davinci.stats.IngestionStats.LEADER_OFFSET_LAG;
import static com.linkedin.davinci.stats.IngestionStats.LEADER_RECORDS_CONSUMED_METRIC_NAME;
import static com.linkedin.davinci.stats.IngestionStats.LEADER_RECORDS_PRODUCED_METRIC_NAME;
import static com.linkedin.davinci.stats.IngestionStats.LEADER_STALLED_HYBRID_INGESTION_METRIC_NAME;
import static com.linkedin.davinci.stats.IngestionStats.NEARLINE_LOCAL_BROKER_TO_READY_TO_SERVE_LATENCY;
import static com.linkedin.davinci.stats.IngestionStats.NEARLINE_PRODUCER_TO_LOCAL_BROKER_LATENCY;
import static com.linkedin.davinci.stats.IngestionStats.OFFSET_REGRESSION_DCR_ERROR;
import static com.linkedin.davinci.stats.IngestionStats.READY_TO_SERVE_WITH_RT_LAG_METRIC_NAME;
import static com.linkedin.davinci.stats.IngestionStats.RECORDS_CONSUMED_METRIC_NAME;
import static com.linkedin.davinci.stats.IngestionStats.STALE_PARTITIONS_WITHOUT_INGESTION_TASK_METRIC_NAME;
import static com.linkedin.davinci.stats.IngestionStats.SUBSCRIBE_ACTION_PREP_LATENCY;
import static com.linkedin.davinci.stats.IngestionStats.TIMESTAMP_REGRESSION_DCR_ERROR;
import static com.linkedin.davinci.stats.IngestionStats.TOMBSTONE_CREATION_DCR;
import static com.linkedin.davinci.stats.IngestionStats.TOTAL_DCR;
import static com.linkedin.davinci.stats.IngestionStats.UPDATE_IGNORED_DCR;
import static com.linkedin.davinci.stats.IngestionStats.VERSION_TOPIC_END_OFFSET_REWIND_COUNT;
import static com.linkedin.davinci.stats.IngestionStats.WRITE_COMPUTE_OPERATION_FAILURE;
import static com.linkedin.venice.stats.StatsErrorCode.NULL_INGESTION_STATS;

import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.stats.Gauge;
import com.linkedin.venice.utils.RegionUtils;
import io.tehuti.metrics.MetricsRepository;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import java.util.function.DoubleSupplier;


/**
 * This class is the reporting class for stats class {@link IngestionStats}.
 * Metrics reporting logics are registered into {@link MetricsRepository} here and send out to external metrics
 * collection/visualization system.
 */
public class IngestionStatsReporter extends AbstractVeniceStatsReporter<IngestionStats> {
  public IngestionStatsReporter(MetricsRepository metricsRepository, String storeName) {
    super(metricsRepository, storeName);
  }

  @Override
  protected void registerStats() {
    registerSensor(
        INGESTION_TASK_ERROR_GAUGE,
        new IngestionStatsGauge(this, () -> (double) getStats().getIngestionTaskErroredGauge()));
    registerSensor(
        INGESTION_TASK_PUSH_TIMEOUT_GAUGE,
        new IngestionStatsGauge(this, () -> (double) getStats().getIngestionTaskPushTimeoutGauge()));
    registerSensor(
        WRITE_COMPUTE_OPERATION_FAILURE,
        new IngestionStatsGauge(this, () -> (double) getStats().getWriteComputeErrorCode()));

    registerSensor(
        FOLLOWER_OFFSET_LAG,
        new IngestionStatsGauge(this, () -> (double) getStats().getFollowerOffsetLag(), 0));
    registerSensor(LEADER_OFFSET_LAG, new IngestionStatsGauge(this, () -> (double) getStats().getLeaderOffsetLag(), 0));

    registerSensor(
        HYBRID_LEADER_OFFSET_LAG,
        new IngestionStatsGauge(this, () -> (double) getStats().getHybridLeaderOffsetLag(), 0));
    registerSensor(
        HYBRID_FOLLOWER_OFFSET_LAG,
        new IngestionStatsGauge(this, () -> (double) getStats().getHybridFollowerOffsetLag(), 0));
    registerSensor(
        VERSION_TOPIC_END_OFFSET_REWIND_COUNT,
        new IngestionStatsGauge(this, () -> getStats().getVersionTopicEndOffsetRewindCount(), 0));

    // System store mostly operates on hybrid partial updates so batch metrics are not useful.
    if (!VeniceSystemStoreUtils.isUserSystemStore(storeName)) {
      registerSensor(
          BATCH_REPLICATION_LAG,
          new IngestionStatsGauge(this, () -> (double) getStats().getBatchReplicationLag(), 0));
      registerSensor(
          BATCH_LEADER_OFFSET_LAG,
          new IngestionStatsGauge(this, () -> (double) getStats().getBatchLeaderOffsetLag(), 0));
      registerSensor(
          BATCH_FOLLOWER_OFFSET_LAG,
          new IngestionStatsGauge(this, () -> (double) getStats().getBatchFollowerOffsetLag(), 0));
    }

    registerSensor(
        RECORDS_CONSUMED_METRIC_NAME,
        new IngestionStatsGauge(this, () -> getStats().getRecordsConsumed(), 0));
    registerSensor(
        LEADER_RECORDS_CONSUMED_METRIC_NAME,
        new IngestionStatsGauge(this, () -> getStats().getLeaderRecordsConsumed(), 0));
    registerSensor(
        FOLLOWER_RECORDS_CONSUMED_METRIC_NAME,
        new IngestionStatsGauge(this, () -> getStats().getFollowerRecordsConsumed(), 0));
    registerSensor(
        LEADER_RECORDS_PRODUCED_METRIC_NAME,
        new IngestionStatsGauge(this, () -> getStats().getLeaderRecordsProduced(), 0));

    // System store does not care about bytes metrics and subscribe latency.
    if (!VeniceSystemStoreUtils.isUserSystemStore(storeName)) {
      registerSensor(BYTES_CONSUMED_METRIC_NAME, new IngestionStatsGauge(this, () -> getStats().getBytesConsumed(), 0));
      registerSensor(
          LEADER_BYTES_CONSUMED_METRIC_NAME,
          new IngestionStatsGauge(this, () -> getStats().getLeaderBytesConsumed(), 0));
      registerSensor(
          FOLLOWER_BYTES_CONSUMED_METRIC_NAME,
          new IngestionStatsGauge(this, () -> getStats().getFollowerBytesConsumed(), 0));
      registerSensor(
          LEADER_BYTES_PRODUCED_METRIC_NAME,
          new IngestionStatsGauge(this, () -> getStats().getLeaderBytesProduced(), 0));
      registerSensor(
          STALE_PARTITIONS_WITHOUT_INGESTION_TASK_METRIC_NAME,
          new IngestionStatsGauge(this, () -> getStats().getStalePartitionsWithoutIngestionTaskCount(), 0));
      registerSensor(
          SUBSCRIBE_ACTION_PREP_LATENCY + "_avg",
          new IngestionStatsGauge(this, () -> getStats().getSubscribePrepLatencyAvg(), 0));
      registerSensor(
          SUBSCRIBE_ACTION_PREP_LATENCY + "_max",
          new IngestionStatsGauge(this, () -> getStats().getSubscribePrepLatencyMax(), 0));
      registerSensor(
          CONSUMED_RECORD_END_TO_END_PROCESSING_LATENCY + "_avg",
          new IngestionStatsGauge(this, () -> getStats().getConsumedRecordEndToEndProcessingLatencyAvg(), 0));
      registerSensor(
          CONSUMED_RECORD_END_TO_END_PROCESSING_LATENCY + "_max",
          new IngestionStatsGauge(this, () -> getStats().getConsumedRecordEndToEndProcessingLatencyMax(), 0));
    }
  }

  // Only register these stats if the store is hybrid.
  @Override
  protected void registerConditionalStats() {
    registerSensor(
        LEADER_STALLED_HYBRID_INGESTION_METRIC_NAME,
        new IngestionStatsGauge(this, () -> getStats().getLeaderStalledHybridIngestion(), 0));
    registerSensor(
        READY_TO_SERVE_WITH_RT_LAG_METRIC_NAME,
        new IngestionStatsGauge(this, () -> getStats().getReadyToServeWithRTLag(), 0));

    if (!VeniceSystemStoreUtils.isSystemStore(storeName)) {
      registerSensor(
          NEARLINE_PRODUCER_TO_LOCAL_BROKER_LATENCY + "_rt_avg",
          new IngestionStatsGauge(this, () -> getStats().getNearlineProducerToLocalBrokerLatencyAvg(), 0));
      registerSensor(
          NEARLINE_PRODUCER_TO_LOCAL_BROKER_LATENCY + "_rt_max",
          new IngestionStatsGauge(this, () -> getStats().getNearlineProducerToLocalBrokerLatencyMax(), 0));
      registerSensor(
          NEARLINE_LOCAL_BROKER_TO_READY_TO_SERVE_LATENCY + "_rt_avg",
          new IngestionStatsGauge(this, () -> getStats().getNearlineLocalBrokerToReadyToServeLatencyAvg(), 0));
      registerSensor(
          NEARLINE_LOCAL_BROKER_TO_READY_TO_SERVE_LATENCY + "_rt_max",
          new IngestionStatsGauge(this, () -> getStats().getNearlineLocalBrokerToReadyToServeLatencyMax(), 0));
    }

    // Do not need to check store name here as per user system store is not in active/active mode.
    if (null != getStats() && getStats().getIngestionTask().isActiveActiveReplicationEnabled()) {
      registerSensor(UPDATE_IGNORED_DCR, new IngestionStatsGauge(this, () -> getStats().getUpdateIgnoredRate(), 0));
      registerSensor(TOTAL_DCR, new IngestionStatsGauge(this, () -> getStats().getTotalDCRRate(), 0));
      registerSensor(
          TOMBSTONE_CREATION_DCR,
          new IngestionStatsGauge(this, () -> getStats().getTombstoneCreationDCRRate(), 0));
      registerSensor(
          TIMESTAMP_REGRESSION_DCR_ERROR,
          new IngestionStatsGauge(this, () -> getStats().getTimestampRegressionDCRRate(), 0));
      registerSensor(
          OFFSET_REGRESSION_DCR_ERROR,
          new IngestionStatsGauge(this, () -> getStats().getOffsetRegressionDCRRate(), 0));

      for (Int2ObjectMap.Entry<String> entry: getStats().getIngestionTask()
          .getServerConfig()
          .getKafkaClusterIdToAliasMap()
          .int2ObjectEntrySet()) {
        int regionId = entry.getIntKey();
        String regionNamePrefix = RegionUtils.getRegionSpecificMetricPrefix(
            getStats().getIngestionTask().getServerConfig().getRegionName(),
            entry.getValue());
        registerSensor(
            regionNamePrefix + "_rt_lag",
            new IngestionStatsGauge(this, () -> (double) getStats().getRegionHybridOffsetLag(regionId), 0));
        registerSensor(
            regionNamePrefix + "_rt_bytes_consumed",
            new IngestionStatsGauge(this, () -> getStats().getRegionHybridBytesConsumed(regionId), 0));
        registerSensor(
            regionNamePrefix + "_rt_records_consumed",
            new IngestionStatsGauge(this, () -> getStats().getRegionHybridRecordsConsumed(regionId), 0));
        registerSensor(
            regionNamePrefix + "_rt_consumed_offset",
            new IngestionStatsGauge(this, () -> getStats().getRegionHybridAvgConsumedOffset(regionId), 0));
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
