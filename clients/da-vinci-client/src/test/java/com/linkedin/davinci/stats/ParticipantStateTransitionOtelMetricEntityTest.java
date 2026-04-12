package com.linkedin.davinci.stats;

import static com.linkedin.davinci.stats.ParticipantStateTransitionOtelMetricEntity.BLOCKED_THREAD_COUNT;
import static com.linkedin.davinci.stats.ParticipantStateTransitionOtelMetricEntity.IN_PROGRESS_COUNT;
import static com.linkedin.davinci.stats.ParticipantStateTransitionOtelMetricEntity.STEADY_STATE_COUNT;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_HELIX_FROM_STATE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_HELIX_STATE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_HELIX_TO_STATE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_THREAD_POOL_NAME;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture.MetricEntityExpectation;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class ParticipantStateTransitionOtelMetricEntityTest {
  @Test
  public void testMetricEntities() {
    new ModuleMetricEntityTestFixture<>(ParticipantStateTransitionOtelMetricEntity.class, expectedDefinitions())
        .assertAll();
  }

  private static Map<ParticipantStateTransitionOtelMetricEntity, MetricEntityExpectation> expectedDefinitions() {
    Map<ParticipantStateTransitionOtelMetricEntity, MetricEntityExpectation> map = new HashMap<>();
    map.put(
        BLOCKED_THREAD_COUNT,
        new MetricEntityExpectation(
            "partition.state.transition.blocked_thread_count",
            MetricType.UP_DOWN_COUNTER,
            MetricUnit.NUMBER,
            "Number of threads currently blocked on a state transition",
            setOf(VENICE_THREAD_POOL_NAME, VENICE_HELIX_FROM_STATE, VENICE_HELIX_TO_STATE)));
    map.put(
        IN_PROGRESS_COUNT,
        new MetricEntityExpectation(
            "partition.state.transition.in_progress_count",
            MetricType.UP_DOWN_COUNTER,
            MetricUnit.NUMBER,
            "Number of partitions currently transitioning between Helix states",
            setOf(VENICE_THREAD_POOL_NAME, VENICE_HELIX_FROM_STATE, VENICE_HELIX_TO_STATE)));
    map.put(
        STEADY_STATE_COUNT,
        new MetricEntityExpectation(
            "partition.state.active_count",
            MetricType.UP_DOWN_COUNTER,
            MetricUnit.NUMBER,
            "Number of partitions currently in a given Helix state",
            setOf(VENICE_THREAD_POOL_NAME, VENICE_HELIX_STATE)));
    return map;
  }
}
