package com.linkedin.davinci.stats;

import static com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface.getUniqueMetricEntities;

import com.linkedin.davinci.stats.ingestion.IngestionOtelStats;
import com.linkedin.davinci.stats.ingestion.heartbeat.HeartbeatOtelStats;
import com.linkedin.davinci.stats.ingestion.heartbeat.RecordLevelDelayOtelStats;
import com.linkedin.venice.stats.metrics.MetricEntity;
import java.util.Collection;


/**
 * Aggregates all metric entities for Venice server (storage node) from module-level inner enums.
 */
public final class ServerMetricEntity {
  public static final Collection<MetricEntity> SERVER_METRIC_ENTITIES = getUniqueMetricEntities(
      IngestionOtelStats.IngestionOtelMetricEntity.class,
      HeartbeatOtelStats.HeartbeatOtelMetricEntity.class,
      RecordLevelDelayOtelStats.RecordLevelDelayOtelMetricEntity.class);

  private ServerMetricEntity() {
  }
}
