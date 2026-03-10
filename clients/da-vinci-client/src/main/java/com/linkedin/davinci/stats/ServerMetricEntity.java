package com.linkedin.davinci.stats;

import static com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface.getUniqueMetricEntities;

import com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity;
import com.linkedin.davinci.stats.ingestion.heartbeat.HeartbeatOtelMetricEntity;
import com.linkedin.davinci.stats.ingestion.heartbeat.RecordLevelDelayOtelMetricEntity;
import com.linkedin.venice.stats.ThreadPoolOtelMetricEntity;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;


/**
 * Aggregates all metric entities for Venice server (storage node) from all server/da-vinci Metric entities.
 *
 * <p>When adding a new {@link ModuleMetricEntityInterface} enum, add it to
 * {@link #getMetricEntityEnumClasses()} — both {@link #SERVER_METRIC_ENTITIES} and tests
 * use this single source of truth.
 */
public final class ServerMetricEntity {
  /**
   * Returns the enum classes that compose {@link #SERVER_METRIC_ENTITIES}. This is the single
   * source of truth used by both production aggregation and tests.
   */
  public static List<Class<? extends ModuleMetricEntityInterface>> getMetricEntityEnumClasses() {
    return Arrays.asList(
        IngestionOtelMetricEntity.class,
        HeartbeatOtelMetricEntity.class,
        RecordLevelDelayOtelMetricEntity.class,
        ServerReadOtelMetricEntity.class,
        ThreadPoolOtelMetricEntity.class,
        ServerMetadataOtelMetricEntity.class,
        ParticipantStoreConsumptionOtelMetricEntity.class,
        AdaptiveThrottlingOtelMetricEntity.class,
        HeartbeatMonitoringOtelMetricEntity.class,
        BlobTransferOtelMetricEntity.class);
  }

  public static final Collection<MetricEntity> SERVER_METRIC_ENTITIES =
      getUniqueMetricEntities(getMetricEntityEnumClasses());

  private ServerMetricEntity() {
  }
}
