package com.linkedin.davinci.stats;

import com.linkedin.venice.helix.HelixState;
import com.linkedin.venice.stats.ThreadPoolStats;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.AsyncGauge;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * This class is used to track the thread pool stats for the state transitions of the participant.
 * Besides the thread pool stats, it also tracks the number of threads that are blocked on the state transition
 * from OFFLINE to DROPPED.
 */
public class ParticipantStateTransitionStats extends ThreadPoolStats {
  private Sensor threadBlockedOnOfflineToDroppedTransitionSensor;
  private AtomicInteger threadBlockedOnOfflineToDroppedTransitionCount = new AtomicInteger(0);

  private final Map<StateTransition, AtomicInteger> inProgressStateTransitionTrackers = new VeniceConcurrentHashMap<>();
  private final Map<String, AtomicInteger> completedStateTransitionTrackers = new VeniceConcurrentHashMap<>();
  private static final Set<String> ENABLED_STEADY_STATES =
      new HashSet<>(Arrays.asList(HelixState.ERROR_STATE, HelixState.STANDBY_STATE, HelixState.LEADER_STATE));

  public ParticipantStateTransitionStats(
      MetricsRepository metricsRepository,
      ThreadPoolExecutor threadPoolExecutor,
      String name) {
    super(metricsRepository, threadPoolExecutor, name);
    threadBlockedOnOfflineToDroppedTransitionSensor = registerSensor(
        new AsyncGauge(
            (ignored, ignored2) -> this.threadBlockedOnOfflineToDroppedTransitionCount.get(),
            "thread_blocked_on_offline_to_dropped_transition"));
  }

  public void incrementThreadBlockedOnOfflineToDroppedTransitionCount() {
    threadBlockedOnOfflineToDroppedTransitionCount.incrementAndGet();
  }

  public void decrementThreadBlockedOnOfflineToDroppedTransitionCount() {
    threadBlockedOnOfflineToDroppedTransitionCount.decrementAndGet();
  }

  public void trackStateTransitionStarted(String fromState, String toState) {
    if (ENABLED_STEADY_STATES.contains(fromState)) {
      getSteadyStateTracker(fromState).decrementAndGet();
    }
    getInProgressStateTransitionTracker(fromState, toState).incrementAndGet();
  }

  public void trackStateTransitionCompleted(String fromState, String toState) {
    getInProgressStateTransitionTracker(fromState, toState).decrementAndGet();
    if (ENABLED_STEADY_STATES.contains(toState)) {
      getSteadyStateTracker(toState).incrementAndGet();
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
