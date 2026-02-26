package com.linkedin.davinci.stats;

import static com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface.getUniqueMetricEntities;

import com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity;
import com.linkedin.davinci.stats.ingestion.heartbeat.HeartbeatOtelMetricEntity;
import com.linkedin.davinci.stats.ingestion.heartbeat.RecordLevelDelayOtelMetricEntity;
import com.linkedin.venice.stats.metrics.MetricEntity;
import java.util.Collection;


/**
 * Aggregates all metric entities for Venice server (storage node) from all server/da-vinci Metric entities
 */
public final class ServerMetricEntity {
  public static final Collection<MetricEntity> SERVER_METRIC_ENTITIES = getUniqueMetricEntities(
      IngestionOtelMetricEntity.class,
      HeartbeatOtelMetricEntity.class,
      RecordLevelDelayOtelMetricEntity.class,
      PubSubHealthOtelMetricEntity.class);

  private ServerMetricEntity() {
  }
}
