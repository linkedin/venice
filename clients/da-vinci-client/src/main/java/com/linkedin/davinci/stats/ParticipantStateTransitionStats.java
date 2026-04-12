package com.linkedin.davinci.stats;

import static com.linkedin.davinci.stats.ParticipantStateTransitionOtelMetricEntity.BLOCKED_THREAD_COUNT;
import static com.linkedin.davinci.stats.ParticipantStateTransitionOtelMetricEntity.IN_PROGRESS_COUNT;
import static com.linkedin.davinci.stats.ParticipantStateTransitionOtelMetricEntity.STEADY_STATE_COUNT;

import com.linkedin.venice.helix.HelixState;
import com.linkedin.venice.stats.OpenTelemetryMetricsSetup;
import com.linkedin.venice.stats.ThreadPoolStats;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceHelixFromState;
import com.linkedin.venice.stats.dimensions.VeniceHelixSteadyState;
import com.linkedin.venice.stats.dimensions.VeniceHelixToState;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.MetricEntityStateOneEnum;
import com.linkedin.venice.stats.metrics.MetricEntityStateTwoEnums;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.AsyncGauge;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Tracks partition state transition metrics for the Helix participant. Extends {@link ThreadPoolStats}
 * (which already has OTel for thread pool metrics) and adds:
 * <ul>
 *   <li>Blocked thread count per (from, to) state pair (currently only OFFLINE→DROPPED)</li>
 *   <li>In-progress partition counts per (from, to) state pair</li>
 *   <li>Active partition counts per Helix state: ERROR, STANDBY, LEADER</li>
 * </ul>
 *
 * <p>All three OTel metrics use UP_DOWN_COUNTER (+1/-1 at call sites). Tehuti uses AsyncGauge
 * polling AtomicInteger trackers. The AtomicIntegers are Tehuti-only artifacts — they will be
 * removed when Tehuti is retired.
 */
public class ParticipantStateTransitionStats extends ThreadPoolStats {
  // Tehuti-only: blocked thread count for OFFLINE→DROPPED (AsyncGauge polls this)
  private final AtomicInteger threadBlockedOnOfflineToDroppedTransitionCount = new AtomicInteger(0);

  /**
   * OTel: blocked thread count with FROM/TO state dimensions (UP_DOWN_COUNTER, records +1/-1).
   * Currently only OFFLINE→DROPPED transitions block threads (waiting for ingestion to stop
   * before dropping a partition). The FROM/TO dimensions allow future transitions to record
   * blocked threads without changing the metric schema.
   */
  private final MetricEntityStateTwoEnums<VeniceHelixFromState, VeniceHelixToState> blockedThreadMetric;

  /** OTel: in-progress transition count with FROM/TO state dimensions (UP_DOWN_COUNTER). */
  private final MetricEntityStateTwoEnums<VeniceHelixFromState, VeniceHelixToState> inProgressMetric;

  /** OTel: active partition count per Helix state with STATE dimension (UP_DOWN_COUNTER). */
  private final MetricEntityStateOneEnum<VeniceHelixSteadyState> steadyStateMetric;

  // Tehuti-only: dynamic AtomicInteger trackers polled by AsyncGauge sensors
  private final Map<StateTransition, AtomicInteger> inProgressStateTransitionTrackers = new VeniceConcurrentHashMap<>();
  private final Map<String, AtomicInteger> completedStateTransitionTrackers = new VeniceConcurrentHashMap<>();
  static final Set<String> ENABLED_STEADY_STATES =
      Utils.setOf(HelixState.ERROR_STATE, HelixState.STANDBY_STATE, HelixState.LEADER_STATE);

  public ParticipantStateTransitionStats(
      MetricsRepository metricsRepository,
      ThreadPoolExecutor threadPoolExecutor,
      String name) {
    super(metricsRepository, threadPoolExecutor, name);

    // Tehuti: fixed AsyncGauge for blocked thread count (OFFLINE→DROPPED only)
    registerSensor(
        new AsyncGauge(
            (ignored, ignored2) -> this.threadBlockedOnOfflineToDroppedTransitionCount.get(),
            "thread_blocked_on_offline_to_dropped_transition"));

    // OTel setup
    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo otelData =
        OpenTelemetryMetricsSetup.builder(metricsRepository).setThreadPoolName(name).build();
    VeniceOpenTelemetryMetricsRepository otelRepository = otelData.getOtelRepository();
    Map<VeniceMetricsDimensions, String> baseDimensionsMap = otelData.getBaseDimensionsMap();

    blockedThreadMetric = MetricEntityStateTwoEnums.create(
        BLOCKED_THREAD_COUNT.getMetricEntity(),
        otelRepository,
        baseDimensionsMap,
        VeniceHelixFromState.class,
        VeniceHelixToState.class);

    inProgressMetric = MetricEntityStateTwoEnums.create(
        IN_PROGRESS_COUNT.getMetricEntity(),
        otelRepository,
        baseDimensionsMap,
        VeniceHelixFromState.class,
        VeniceHelixToState.class);

    steadyStateMetric = MetricEntityStateOneEnum
        .create(STEADY_STATE_COUNT.getMetricEntity(), otelRepository, baseDimensionsMap, VeniceHelixSteadyState.class);
  }

  /** Currently only OFFLINE→DROPPED blocks threads; pass different enum values for future transitions. */
  public void incrementThreadBlockedOnOfflineToDroppedTransitionCount() {
    threadBlockedOnOfflineToDroppedTransitionCount.incrementAndGet();
    blockedThreadMetric.record(1, VeniceHelixFromState.OFFLINE, VeniceHelixToState.DROPPED);
  }

  /** Currently only OFFLINE→DROPPED blocks threads; pass different enum values for future transitions. */
  public void decrementThreadBlockedOnOfflineToDroppedTransitionCount() {
    threadBlockedOnOfflineToDroppedTransitionCount.decrementAndGet();
    blockedThreadMetric.record(-1, VeniceHelixFromState.OFFLINE, VeniceHelixToState.DROPPED);
  }

  public void trackStateTransitionStarted(String fromState, String toState) {
    if (ENABLED_STEADY_STATES.contains(fromState)) {
      getSteadyStateTracker(fromState).decrementAndGet();
      recordSteadyStateOtel(-1, fromState);
    }
    getInProgressStateTransitionTracker(fromState, toState).incrementAndGet();
    recordInProgressOtel(1, fromState, toState);
  }

  public void trackStateTransitionCompleted(String fromState, String toState) {
    getInProgressStateTransitionTracker(fromState, toState).decrementAndGet();
    recordInProgressOtel(-1, fromState, toState);
    if (ENABLED_STEADY_STATES.contains(toState)) {
      getSteadyStateTracker(toState).incrementAndGet();
      recordSteadyStateOtel(1, toState);
    }
  }

  private void recordInProgressOtel(long delta, String fromState, String toState) {
    try {
      VeniceHelixFromState from = VeniceHelixFromState.valueOf(fromState);
      VeniceHelixToState to = VeniceHelixToState.valueOf(toState);
      inProgressMetric.record(delta, from, to);
    } catch (IllegalArgumentException e) {
      // Defensive: in practice unreachable because Helix dispatches state transitions via
      // explicitly named handler methods (e.g., onBecomeStandbyFromOffline). A new Helix state
      // without a handler would fail before reaching this code. Kept as a safety net.
    }
  }

  private void recordSteadyStateOtel(long delta, String state) {
    try {
      VeniceHelixSteadyState steadyState = VeniceHelixSteadyState.valueOf(state);
      steadyStateMetric.record(delta, steadyState);
    } catch (IllegalArgumentException e) {
      // Defensive: same reasoning as recordInProgressOtel — unreachable in practice.
    }
  }

  private AtomicInteger getInProgressStateTransitionTracker(String fromState, String toState) {
    return inProgressStateTransitionTrackers.computeIfAbsent(new StateTransition(fromState, toState), key -> {
      AtomicInteger tracker = new AtomicInteger(0);
      registerInProgressTransitionSensorIfAbsent(fromState, toState, tracker);
      return tracker;
    });
  }

  private AtomicInteger getSteadyStateTracker(String state) {
    return completedStateTransitionTrackers.computeIfAbsent(state, key -> {
      AtomicInteger tracker = new AtomicInteger(0);
      registerSteadyStateSensorIfAbsent(state, tracker);
      return tracker;
    });
  }

  private void registerInProgressTransitionSensorIfAbsent(String fromState, String toState, AtomicInteger tracker) {
    String sensorName = String.format("num_partition_in_transition_from_%s_to_%s", fromState, toState);
    registerSensorIfAbsent(new AsyncGauge((ignored, ignored2) -> tracker.get(), sensorName));
  }

  private void registerSteadyStateSensorIfAbsent(String state, AtomicInteger tracker) {
    String sensorName = String.format("num_partition_in_%s_state", state);
    registerSensorIfAbsent(new AsyncGauge((ignored, ignored2) -> tracker.get(), sensorName));
  }

  private static class StateTransition {
    private final String fromState;
    private final String toState;

    public StateTransition(String fromState, String toState) {
      this.fromState = fromState;
      this.toState = toState;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (!(o instanceof StateTransition))
        return false;
      StateTransition that = (StateTransition) o;
      return fromState.equals(that.fromState) && toState.equals(that.toState);
    }

    @Override
    public int hashCode() {
      return Objects.hash(fromState, toState);
    }
  }
}
