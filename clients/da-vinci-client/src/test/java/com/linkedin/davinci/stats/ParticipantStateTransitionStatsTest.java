package com.linkedin.davinci.stats;

import static com.linkedin.venice.helix.HelixState.LEADER_STATE;
import static com.linkedin.venice.helix.HelixState.OFFLINE_STATE;
import static com.linkedin.venice.helix.HelixState.STANDBY_STATE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import io.tehuti.Metric;
import io.tehuti.metrics.MetricsRepository;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class ParticipantStateTransitionStatsTest {
  private ParticipantStateTransitionStats stats;
  private MetricsRepository metricsRepository;

  private static final String METRIC_PREFIX = "S_T_Metric_Test";

  @BeforeClass
  public void setUp() {
    metricsRepository = new MetricsRepository();
    ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
    stats = new ParticipantStateTransitionStats(metricsRepository, executor, METRIC_PREFIX);
  }

  @Test
  public void testTrackStateTransitionStartedAndCompleted() {
    // Test transition from OFFLINE to STANDBY
    String offlineToStandbyMetricName = getInProgressTransitionMetricName(OFFLINE_STATE, STANDBY_STATE);
    assertNull(metricsRepository.getMetric(offlineToStandbyMetricName));

    // Simulate a transition from OFFLINE to STANDBY
    stats.trackStateTransitionStarted(OFFLINE_STATE, STANDBY_STATE);

    // Verify that the in-progress metric for OFFLINE to STANDBY is created
    Metric offlineToStandby = metricsRepository.getMetric(offlineToStandbyMetricName);
    assertNotNull(offlineToStandby);
    assertEquals(offlineToStandby.value(), 1.0);

    String offlineStateMetricName = getSteadyStateMetricName(OFFLINE_STATE);
    assertNull(
        metricsRepository.getMetric(offlineStateMetricName),
        "Steady state metrics should only exist for ERROR, LEADER and STANDBY states");

    // Complete the transition to STANDBY
    stats.trackStateTransitionCompleted(OFFLINE_STATE, STANDBY_STATE);

    // Verify that the in-progress metric for OFFLINE to STANDBY is decremented
    assertEquals(offlineToStandby.value(), 0.0);

    // Verify that the steady state metric for STANDBY is incremented
    String standbyStateMetricName = getSteadyStateMetricName(STANDBY_STATE);
    Metric steadyStateStandby = metricsRepository.getMetric(standbyStateMetricName);
    assertNotNull(steadyStateStandby);
    assertEquals(steadyStateStandby.value(), 1.0);

    // Test transition from STANDBY to LEADER
    String standbyToLeaderMetricName = getInProgressTransitionMetricName(STANDBY_STATE, LEADER_STATE);
    assertNull(metricsRepository.getMetric(standbyToLeaderMetricName));

    // Simulate a transition from STANDBY to LEADER
    stats.trackStateTransitionStarted(STANDBY_STATE, LEADER_STATE);

    // Verify that the in-progress metric for STANDBY to LEADER is created
    Metric standbyToLeader = metricsRepository.getMetric(standbyToLeaderMetricName);
    assertNotNull(standbyToLeader);
    assertEquals(standbyToLeader.value(), 1.0, "In-progress transition from STANDBY to LEADER should be incremented");
    assertEquals(steadyStateStandby.value(), 0.0, "STANDBY state should be decremented when transitioning to LEADER");

    String leaderStateMetricName = getSteadyStateMetricName(LEADER_STATE);
    assertNull(metricsRepository.getMetric(leaderStateMetricName));

    // Complete the transition to LEADER
    stats.trackStateTransitionCompleted(STANDBY_STATE, LEADER_STATE);
    assertEquals(standbyToLeader.value(), 0.0, "In-progress transition from STANDBY to LEADER should be decremented");

    // Verify that the steady state metric for LEADER is incremented
    Metric steadyStateLeader = metricsRepository.getMetric(leaderStateMetricName);
    assertNotNull(steadyStateLeader);
    assertEquals(steadyStateLeader.value(), 1.0, "LEADER state should be incremented after transition from STANDBY");
  }

  private String getInProgressTransitionMetricName(String fromState, String toState) {
    return String.format(".%s--num_partition_in_transition_from_%s_to_%s.Gauge", METRIC_PREFIX, fromState, toState);
  }

  private String getSteadyStateMetricName(String state) {
    return String.format(".%s--num_partition_in_%s_state.Gauge", METRIC_PREFIX, state);
  }
}
