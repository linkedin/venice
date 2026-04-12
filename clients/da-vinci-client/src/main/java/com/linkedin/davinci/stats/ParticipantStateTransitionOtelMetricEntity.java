package com.linkedin.davinci.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_HELIX_FROM_STATE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_HELIX_STATE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_HELIX_TO_STATE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_THREAD_POOL_NAME;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface;
import java.util.Set;


/**
 * OTel metric entity definitions for {@link ParticipantStateTransitionStats}.
 * Tracks partition state transition lifecycle and active partition counts per Helix state.
 */
public enum ParticipantStateTransitionOtelMetricEntity implements ModuleMetricEntityInterface {
  BLOCKED_THREAD_COUNT(
      "partition.state.transition.blocked_thread_count", MetricType.UP_DOWN_COUNTER, MetricUnit.NUMBER,
      "Number of threads currently blocked on a state transition",
      setOf(VENICE_THREAD_POOL_NAME, VENICE_HELIX_FROM_STATE, VENICE_HELIX_TO_STATE)
  ),

  IN_PROGRESS_COUNT(
      "partition.state.transition.in_progress_count", MetricType.UP_DOWN_COUNTER, MetricUnit.NUMBER,
      "Number of partitions currently transitioning between Helix states",
      setOf(VENICE_THREAD_POOL_NAME, VENICE_HELIX_FROM_STATE, VENICE_HELIX_TO_STATE)
  ),

  STEADY_STATE_COUNT(
      "partition.state.active_count", MetricType.UP_DOWN_COUNTER, MetricUnit.NUMBER,
      "Number of partitions currently in a given Helix state", setOf(VENICE_THREAD_POOL_NAME, VENICE_HELIX_STATE)
  );

  private final MetricEntity metricEntity;

  ParticipantStateTransitionOtelMetricEntity(
      String metricName,
      MetricType metricType,
      MetricUnit unit,
      String description,
      Set<VeniceMetricsDimensions> dimensions) {
    this.metricEntity = new MetricEntity(metricName, metricType, unit, description, dimensions);
  }

  @Override
  public MetricEntity getMetricEntity() {
    return metricEntity;
  }
}
