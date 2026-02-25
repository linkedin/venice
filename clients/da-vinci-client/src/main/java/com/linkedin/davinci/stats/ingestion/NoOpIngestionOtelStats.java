package com.linkedin.davinci.stats.ingestion;

import com.linkedin.davinci.kafka.consumer.StoreIngestionTask;
import com.linkedin.venice.stats.dimensions.ReplicaType;
import com.linkedin.venice.stats.dimensions.VeniceDCREvent;
import com.linkedin.venice.stats.dimensions.VeniceIngestionDestinationComponent;
import com.linkedin.venice.stats.dimensions.VeniceIngestionSourceComponent;
import com.linkedin.venice.stats.dimensions.VeniceRegionLocality;


/**
 * A no-op implementation of {@link IngestionOtelStats} that discards all recording calls.
 * Used when ingestion OTel stats are disabled via config so that all recording call sites in
 * {@link com.linkedin.davinci.stats.AggVersionedIngestionStats} stay as simple unconditional calls
 * instead of being wrapped in null checks. This is a readability/maintainability choice, not a
 * performance one — JIT would optimize null checks equally well via constant-folding of the
 * {@code final boolean} guard in {@code getIngestionOtelStats()}.
 */
public class NoOpIngestionOtelStats extends IngestionOtelStats {
  /** Singleton instance — safe to share since all methods are stateless no-ops. */
  public static final NoOpIngestionOtelStats INSTANCE = new NoOpIngestionOtelStats();

  NoOpIngestionOtelStats() {
    super();
  }

  @Override
  public void setIngestionTask(int version, StoreIngestionTask task) {
  }

  @Override
  public void removeIngestionTask(int version) {
  }

  @Override
  public void close() {
  }

  @Override
  public void setIngestionTaskPushTimeoutGauge(int version, int value) {
  }

  @Override
  public void recordIdleTime(int version, long idleTimeMs) {
  }

  @Override
  public boolean emitOtelMetrics() {
    return false;
  }

  @Override
  public void updateVersionInfo(int currentVersion, int futureVersion) {
  }

  @Override
  public void recordSubscribePrepTime(int version, double latencyMs) {
  }

  @Override
  public void recordIngestionTime(int version, double latencyMs) {
  }

  @Override
  public void recordPreprocessingLeaderTime(int version, double latencyMs) {
  }

  @Override
  public void recordPreprocessingInternalTime(int version, double latencyMs) {
  }

  @Override
  public void recordProducerTime(int version, double latencyMs) {
  }

  @Override
  public void recordBatchProcessingRequestCount(int version, long value) {
  }

  @Override
  public void recordBatchProcessingRequestRecordCount(int version, long value) {
  }

  @Override
  public void recordBatchProcessingRequestErrorCount(int version, long value) {
  }

  @Override
  public void recordBatchProcessingRequestTime(int version, double latencyMs) {
  }

  @Override
  public void recordDcrTotalCount(int version, long value) {
  }

  @Override
  public void recordDuplicateKeyUpdateCount(int version, long value) {
  }

  @Override
  public void recordRecordsConsumed(int version, ReplicaType replicaType, long value) {
  }

  @Override
  public void recordRecordsProduced(int version, ReplicaType replicaType, long value) {
  }

  @Override
  public void recordBytesConsumed(int version, ReplicaType replicaType, long value) {
  }

  @Override
  public void recordBytesProduced(int version, ReplicaType replicaType, long value) {
  }

  @Override
  public void recordProducerCallbackTime(int version, ReplicaType replicaType, double latencyMs) {
  }

  @Override
  public void recordDcrEventCount(int version, VeniceDCREvent event, long value) {
  }

  @Override
  public void recordTimeBetweenComponents(
      int version,
      VeniceIngestionSourceComponent sourceComponent,
      VeniceIngestionDestinationComponent destComponent,
      double latencyMs) {
  }

  @Override
  public void recordRtRecordsConsumed(
      int version,
      String sourceRegion,
      VeniceRegionLocality regionLocality,
      long count) {
  }

  @Override
  public void recordRtBytesConsumed(int version, String sourceRegion, VeniceRegionLocality regionLocality, long bytes) {
  }
}
