package com.linkedin.davinci.stats;

import com.linkedin.venice.stats.LambdaStat;
import com.linkedin.venice.stats.ThreadPoolStats;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
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

  public ParticipantStateTransitionStats(
      MetricsRepository metricsRepository,
      ThreadPoolExecutor threadPoolExecutor,
      String name) {
    super(metricsRepository, threadPoolExecutor, name);
    threadBlockedOnOfflineToDroppedTransitionSensor = registerSensor(
        "thread_blocked_on_offline_to_dropped_transition",
        new LambdaStat(() -> this.threadBlockedOnOfflineToDroppedTransitionCount.get()));
  }

  public void incrementThreadBlockedOnOfflineToDroppedTransitionCount() {
    threadBlockedOnOfflineToDroppedTransitionCount.incrementAndGet();
  }

  public void decrementThreadBlockedOnOfflineToDroppedTransitionCount() {
    threadBlockedOnOfflineToDroppedTransitionCount.decrementAndGet();
  }
}
