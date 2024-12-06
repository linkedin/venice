package com.linkedin.davinci.stats;

import static com.linkedin.davinci.stats.IngestionStats.BATCH_FOLLOWER_OFFSET_LAG;
import static com.linkedin.davinci.stats.IngestionStats.BATCH_LEADER_OFFSET_LAG;
import static com.linkedin.davinci.stats.IngestionStats.BATCH_PROCESSING_REQUEST;
import static com.linkedin.davinci.stats.IngestionStats.BATCH_PROCESSING_REQUEST_ERROR;
import static com.linkedin.davinci.stats.IngestionStats.BATCH_PROCESSING_REQUEST_LATENCY;
import static com.linkedin.davinci.stats.IngestionStats.BATCH_PROCESSING_REQUEST_RECORDS;
import static com.linkedin.davinci.stats.IngestionStats.BATCH_PROCESSING_REQUEST_SIZE;
import static com.linkedin.davinci.stats.IngestionStats.BATCH_REPLICATION_LAG;
import static com.linkedin.davinci.stats.IngestionStats.BYTES_CONSUMED_METRIC_NAME;
import static com.linkedin.davinci.stats.IngestionStats.CONSUMED_RECORD_END_TO_END_PROCESSING_LATENCY;
import static com.linkedin.davinci.stats.IngestionStats.FOLLOWER_BYTES_CONSUMED_METRIC_NAME;
import static com.linkedin.davinci.stats.IngestionStats.FOLLOWER_OFFSET_LAG;
import static com.linkedin.davinci.stats.IngestionStats.FOLLOWER_RECORDS_CONSUMED_METRIC_NAME;
import static com.linkedin.davinci.stats.IngestionStats.HYBRID_FOLLOWER_OFFSET_LAG;
import static com.linkedin.davinci.stats.IngestionStats.HYBRID_LEADER_OFFSET_LAG;
import static com.linkedin.davinci.stats.IngestionStats.IDLE_TIME;
import static com.linkedin.davinci.stats.IngestionStats.INGESTION_TASK_ERROR_GAUGE;
import static com.linkedin.davinci.stats.IngestionStats.INGESTION_TASK_PUSH_TIMEOUT_GAUGE;
import static com.linkedin.davinci.stats.IngestionStats.INTERNAL_PREPROCESSING_LATENCY;
import static com.linkedin.davinci.stats.IngestionStats.LEADER_BYTES_CONSUMED_METRIC_NAME;
import static com.linkedin.davinci.stats.IngestionStats.LEADER_BYTES_PRODUCED_METRIC_NAME;
import static com.linkedin.davinci.stats.IngestionStats.LEADER_OFFSET_LAG;
import static com.linkedin.davinci.stats.IngestionStats.LEADER_PREPROCESSING_LATENCY;
import static com.linkedin.davinci.stats.IngestionStats.LEADER_RECORDS_CONSUMED_METRIC_NAME;
import static com.linkedin.davinci.stats.IngestionStats.LEADER_RECORDS_PRODUCED_METRIC_NAME;
import static com.linkedin.davinci.stats.IngestionStats.LEADER_STALLED_HYBRID_INGESTION_METRIC_NAME;
import static com.linkedin.davinci.stats.IngestionStats.NEARLINE_LOCAL_BROKER_TO_READY_TO_SERVE_LATENCY;
import static com.linkedin.davinci.stats.IngestionStats.NEARLINE_PRODUCER_TO_LOCAL_BROKER_LATENCY;
import static com.linkedin.davinci.stats.IngestionStats.OFFSET_REGRESSION_DCR_ERROR;
import static com.linkedin.davinci.stats.IngestionStats.PRODUCER_CALLBACK_LATENCY;
import static com.linkedin.davinci.stats.IngestionStats.READY_TO_SERVE_WITH_RT_LAG_METRIC_NAME;
import static com.linkedin.davinci.stats.IngestionStats.RECORDS_CONSUMED_METRIC_NAME;
import static com.linkedin.davinci.stats.IngestionStats.SUBSCRIBE_ACTION_PREP_LATENCY;
import static com.linkedin.davinci.stats.IngestionStats.TIMESTAMP_REGRESSION_DCR_ERROR;
import static com.linkedin.davinci.stats.IngestionStats.TOMBSTONE_CREATION_DCR;
import static com.linkedin.davinci.stats.IngestionStats.TOTAL_DCR;
import static com.linkedin.davinci.stats.IngestionStats.UPDATE_IGNORED_DCR;
import static com.linkedin.davinci.stats.IngestionStats.VERSION_TOPIC_END_OFFSET_REWIND_COUNT;
import static com.linkedin.davinci.stats.IngestionStats.WRITE_COMPUTE_OPERATION_FAILURE;
import static com.linkedin.venice.stats.StatsErrorCode.NULL_INGESTION_STATS;

import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.utils.RegionUtils;
import com.linkedin.venice.utils.Utils;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.AsyncGauge;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import java.util.function.DoubleSupplier;
import java.util.function.Function;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class is the reporting class for stats class {@link IngestionStats}.
 * Metrics reporting logics are registered into {@link MetricsRepository} here and send out to external metrics
 * collection/visualization system.
 */
public class IngestionStatsReporter extends AbstractVeniceStatsReporter<IngestionStats> {
  private static final Logger LOGGER = LogManager.getLogger(IngestionStatsReporter.class);

  public IngestionStatsReporter(MetricsRepository metricsRepository, String storeName, String clusterName) {
    super(metricsRepository, storeName);
  }

  @Override
  protected void registerStats() {
    registerSensor(
        new IngestionStatsGauge(
            this,
            () -> (double) getStats().getIngestionTaskErroredGauge(),
            INGESTION_TASK_ERROR_GAUGE));
    registerSensor(
        new IngestionStatsGauge(
            this,
            () -> (double) getStats().getIngestionTaskPushTimeoutGauge(),
            INGESTION_TASK_PUSH_TIMEOUT_GAUGE));
    registerSensor(
        new IngestionStatsGauge(
            this,
            () -> (double) getStats().getWriteComputeErrorCode(),
            WRITE_COMPUTE_OPERATION_FAILURE));

    registerSensor(
        new IngestionStatsGauge(this, () -> (double) getStats().getFollowerOffsetLag(), 0, FOLLOWER_OFFSET_LAG));
    registerSensor(new IngestionStatsGauge(this, () -> (double) getStats().getLeaderOffsetLag(), 0, LEADER_OFFSET_LAG));

    registerSensor(
        new IngestionStatsGauge(
            this,
            () -> (double) getStats().getHybridLeaderOffsetLag(),
            0,
            HYBRID_LEADER_OFFSET_LAG));
    registerSensor(
        new IngestionStatsGauge(
            this,
            () -> (double) getStats().getHybridFollowerOffsetLag(),
            0,
            HYBRID_FOLLOWER_OFFSET_LAG));
    registerSensor(
        new IngestionStatsGauge(
            this,
            () -> getStats().getVersionTopicEndOffsetRewindCount(),
            0,
            VERSION_TOPIC_END_OFFSET_REWIND_COUNT));

    registerSensor(
        new IngestionStatsGauge(this, () -> getStats().getRecordsConsumed(), 0, RECORDS_CONSUMED_METRIC_NAME));
    registerSensor(
        new IngestionStatsGauge(
            this,
            () -> getStats().getLeaderRecordsConsumed(),
            0,
            LEADER_RECORDS_CONSUMED_METRIC_NAME));
    registerSensor(
        new IngestionStatsGauge(
            this,
            () -> getStats().getFollowerRecordsConsumed(),
            0,
            FOLLOWER_RECORDS_CONSUMED_METRIC_NAME));
    registerSensor(
        new IngestionStatsGauge(
            this,
            () -> getStats().getLeaderRecordsProduced(),
            0,
            LEADER_RECORDS_PRODUCED_METRIC_NAME));

    // System store does not care about bytes metrics and subscribe latency.
    if (!VeniceSystemStoreUtils.isUserSystemStore(storeName)) {
      registerSensor(new IngestionStatsGauge(this, () -> getStats().getBytesConsumed(), 0, BYTES_CONSUMED_METRIC_NAME));
      registerSensor(
          new IngestionStatsGauge(
              this,
              () -> getStats().getLeaderBytesConsumed(),
              0,
              LEADER_BYTES_CONSUMED_METRIC_NAME));
      registerSensor(
          new IngestionStatsGauge(
              this,
              () -> getStats().getFollowerBytesConsumed(),
              0,
              FOLLOWER_BYTES_CONSUMED_METRIC_NAME));
      registerSensor(
          new IngestionStatsGauge(
              this,
              () -> getStats().getLeaderBytesProduced(),
              0,
              LEADER_BYTES_PRODUCED_METRIC_NAME));
      registerSensor(
          new IngestionStatsGauge(
              this,
              () -> getStats().getSubscribePrepLatencyAvg(),
              0,
              SUBSCRIBE_ACTION_PREP_LATENCY + "_avg"));
      registerSensor(
          new IngestionStatsGauge(
              this,
              () -> getStats().getSubscribePrepLatencyMax(),
              0,
              SUBSCRIBE_ACTION_PREP_LATENCY + "_max"));
      registerSensor(
          new IngestionStatsGauge(
              this,
              () -> getStats().getConsumedRecordEndToEndProcessingLatencyAvg(),
              0,
              CONSUMED_RECORD_END_TO_END_PROCESSING_LATENCY + "_avg"));
      registerSensor(
          new IngestionStatsGauge(
              this,
              () -> getStats().getConsumedRecordEndToEndProcessingLatencyMax(),
              0,
              CONSUMED_RECORD_END_TO_END_PROCESSING_LATENCY + "_max"));
      registerSensor(new IngestionStatsGauge(this, () -> getStats().getIdleTime(), 0, IDLE_TIME + "_max"));
      registerSensor(
          new IngestionStatsGauge(
              this,
              () -> getStats().getProducerCallBackLatencyMax(),
              0,
              PRODUCER_CALLBACK_LATENCY + "_max"));

      registerSensor(
          new IngestionStatsGauge(
              this,
              () -> getStats().getLeaderPreprocessingLatencyMax(),
              0,
              LEADER_PREPROCESSING_LATENCY + "_max"));

      registerSensor(
          new IngestionStatsGauge(
              this,
              () -> getStats().getInternalPreprocessingLatencyMax(),
              0,
              INTERNAL_PREPROCESSING_LATENCY + "_max"));

      registerSensor(
          new IngestionStatsGauge(
              this,
              () -> getStats().getLeaderPreprocessingLatencyAvg(),
              0,
              LEADER_PREPROCESSING_LATENCY + "_avg"));

      registerSensor(
          new IngestionStatsGauge(
              this,
              () -> getStats().getInternalPreprocessingLatencyAvg(),
              0,
              INTERNAL_PREPROCESSING_LATENCY + "_avg"));

      registerSensor(
          new IngestionStatsGauge(this, () -> (double) getStats().getBatchReplicationLag(), 0, BATCH_REPLICATION_LAG));
      registerSensor(
          new IngestionStatsGauge(
              this,
              () -> (double) getStats().getBatchLeaderOffsetLag(),
              0,
              BATCH_LEADER_OFFSET_LAG));
      registerSensor(
          new IngestionStatsGauge(
              this,
              () -> (double) getStats().getBatchFollowerOffsetLag(),
              0,
              BATCH_FOLLOWER_OFFSET_LAG));

      registerLatencySensor("producer_to_source_broker", IngestionStats::getProducerSourceBrokerLatencySensor);
      registerLatencySensor(
          "source_broker_to_leader_consumer",
          IngestionStats::getSourceBrokerLeaderConsumerLatencySensor);
      registerLatencySensor("producer_to_local_broker", IngestionStats::getProducerLocalBrokerLatencySensor);
      registerLatencySensor(
          "local_broker_to_follower_consumer",
          IngestionStats::getLocalBrokerFollowerConsumerLatencySensor);
      registerLatencySensor("leader_producer_completion", IngestionStats::getLeaderProducerCompletionLatencySensor);

      registerSensor(
          new IngestionStatsGauge(this, () -> getStats().getBatchProcessingRequest(), 0, BATCH_PROCESSING_REQUEST));
      registerSensor(
          new IngestionStatsGauge(
              this,
              () -> getStats().getBatchProcessingRequestError(),
              BATCH_PROCESSING_REQUEST_ERROR));
      registerSensor(
          new IngestionStatsGauge(
              this,
              () -> getStats().getBatchProcessingRequestRecords(),
              0,
              BATCH_PROCESSING_REQUEST_RECORDS));
      registerSensor(
          new IngestionStatsGauge(
              this,
              () -> getStats().getBatchProcessingRequestSizeSensor().getAvg(),
              0,
              BATCH_PROCESSING_REQUEST_SIZE + "_avg"));
      registerSensor(
          new IngestionStatsGauge(
              this,
              () -> getStats().getBatchProcessingRequestSizeSensor().getMax(),
              0,
              BATCH_PROCESSING_REQUEST_SIZE + "_max"));
      registerSensor(
          new IngestionStatsGauge(
              this,
              () -> getStats().getBatchProcessingRequestLatencySensor().getAvg(),
              0,
              BATCH_PROCESSING_REQUEST_LATENCY + "_avg"));
      registerSensor(
          new IngestionStatsGauge(
              this,
              () -> getStats().getBatchProcessingRequestLatencySensor().getMax(),
              0,
              BATCH_PROCESSING_REQUEST_LATENCY + "_max"));
    }
  }

  // Only register these stats if the store is hybrid.
  @Override
  protected void registerConditionalStats() {
    registerSensor(
        new IngestionStatsGauge(
            this,
            () -> getStats().getLeaderStalledHybridIngestion(),
            0,
            LEADER_STALLED_HYBRID_INGESTION_METRIC_NAME));
    registerSensor(
        new IngestionStatsGauge(
            this,
            () -> getStats().getReadyToServeWithRTLag(),
            0,
            READY_TO_SERVE_WITH_RT_LAG_METRIC_NAME));

    if (!VeniceSystemStoreUtils.isSystemStore(storeName)) {
      registerSensor(
          new IngestionStatsGauge(
              this,
              () -> getStats().getNearlineProducerToLocalBrokerLatencyAvg(),
              0,
              NEARLINE_PRODUCER_TO_LOCAL_BROKER_LATENCY + "_rt_avg"));
      registerSensor(
          new IngestionStatsGauge(
              this,
              () -> getStats().getNearlineProducerToLocalBrokerLatencyMax(),
              0,
              NEARLINE_PRODUCER_TO_LOCAL_BROKER_LATENCY + "_rt_max"));
      registerSensor(
          new IngestionStatsGauge(
              this,
              () -> getStats().getNearlineLocalBrokerToReadyToServeLatencyAvg(),
              0,
              NEARLINE_LOCAL_BROKER_TO_READY_TO_SERVE_LATENCY + "_rt_avg"));
      registerSensor(
          new IngestionStatsGauge(
              this,
              () -> getStats().getNearlineLocalBrokerToReadyToServeLatencyMax(),
              0,
              NEARLINE_LOCAL_BROKER_TO_READY_TO_SERVE_LATENCY + "_rt_max"));
    }

    if (getStats() == null) {
      LOGGER.warn("Failed to fully registerConditionalStats because getStats() returns null for: {}", storeName);
      return;
    } else if (getStats().getIngestionTask() == null) {
      LOGGER.warn(
          "Failed to fully registerConditionalStats because getStats().getIngestionTask() returns null for: {}",
          storeName);
      return;
    }

    // Do not need to check store name here as per user system store is not in active/active mode.
    if (null != getStats() && getStats().getIngestionTask().isActiveActiveReplicationEnabled()) {
      registerSensor(new IngestionStatsGauge(this, () -> getStats().getUpdateIgnoredRate(), 0, UPDATE_IGNORED_DCR));
      registerSensor(new IngestionStatsGauge(this, () -> getStats().getTotalDCRRate(), 0, TOTAL_DCR));
      registerSensor(
          new IngestionStatsGauge(this, () -> getStats().getTombstoneCreationDCRRate(), 0, TOMBSTONE_CREATION_DCR));
      registerSensor(
          new IngestionStatsGauge(
              this,
              () -> getStats().getTimestampRegressionDCRRate(),
              0,
              TIMESTAMP_REGRESSION_DCR_ERROR));
      registerSensor(
          new IngestionStatsGauge(this, () -> getStats().getOffsetRegressionDCRRate(), 0, OFFSET_REGRESSION_DCR_ERROR));

      for (Int2ObjectMap.Entry<String> entry: getStats().getIngestionTask()
          .getServerConfig()
          .getKafkaClusterIdToAliasMap()
          .int2ObjectEntrySet()) {
        // We will only register sensor for SIT with separate RT topic enabled to avoid unnecessary metrics.
        if (!getStats().getIngestionTask().isSeparatedRealtimeTopicEnabled()
            && Utils.isSeparateTopicRegion(entry.getValue())) {
          continue;
        }
        int regionId = entry.getIntKey();
        String regionNamePrefix = RegionUtils.getRegionSpecificMetricPrefix(
            getStats().getIngestionTask().getServerConfig().getRegionName(),
            entry.getValue());
        registerSensor(
            new IngestionStatsGauge(
                this,
                () -> (double) getStats().getRegionHybridOffsetLag(regionId),
                0,
                regionNamePrefix + "_rt_lag"));
        registerSensor(
            new IngestionStatsGauge(
                this,
                () -> getStats().getRegionHybridBytesConsumed(regionId),
                0,
                regionNamePrefix + "_rt_bytes_consumed"));
        registerSensor(
            new IngestionStatsGauge(
                this,
                () -> getStats().getRegionHybridRecordsConsumed(regionId),
                0,
                regionNamePrefix + "_rt_records_consumed"));
        registerSensor(
            new IngestionStatsGauge(
                this,
                () -> getStats().getRegionHybridAvgConsumedOffset(regionId),
                0,
                regionNamePrefix + "_rt_consumed_offset"));
      }
    }
  }

  protected void registerLatencySensor(
      String sensorBaseName,
      Function<IngestionStats, WritePathLatencySensor> sensorFunction) {
    registerSensor(
        new IngestionStatsGauge(
            this,
            () -> sensorFunction.apply(getStats()).getAvg(),
            sensorBaseName + "_latency_avg_ms"));
    registerSensor(
        new IngestionStatsGauge(
            this,
            () -> sensorFunction.apply(getStats()).getMax(),
            sensorBaseName + "_latency_max_ms"));
  }

  protected static class IngestionStatsGauge extends AsyncGauge {
    IngestionStatsGauge(AbstractVeniceStatsReporter reporter, DoubleSupplier supplier, String metricName) {
      this(reporter, supplier, NULL_INGESTION_STATS.code, metricName);
    }

    IngestionStatsGauge(
        AbstractVeniceStatsReporter reporter,
        DoubleSupplier supplier,
        int defaultValue,
        String metricName) {
      /**
       * If a version doesn't exist, the corresponding reporter stat doesn't exist after the host restarts,
       * which is not an error. The users of the stats should decide whether it's reasonable to emit an error
       * code simply because the version is not created yet.
       */
      super((ignored, ignored2) -> reporter.getStats() == null ? defaultValue : supplier.getAsDouble(), metricName);
    }
  }
}
