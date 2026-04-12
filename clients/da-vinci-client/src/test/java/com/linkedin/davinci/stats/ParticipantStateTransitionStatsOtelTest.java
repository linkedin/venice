package com.linkedin.davinci.stats;

import static com.linkedin.davinci.stats.ParticipantStateTransitionOtelMetricEntity.BLOCKED_THREAD_COUNT;
import static com.linkedin.davinci.stats.ParticipantStateTransitionOtelMetricEntity.IN_PROGRESS_COUNT;
import static com.linkedin.davinci.stats.ParticipantStateTransitionOtelMetricEntity.STEADY_STATE_COUNT;
import static com.linkedin.davinci.stats.ServerMetricEntity.SERVER_METRIC_ENTITIES;
import static com.linkedin.venice.helix.HelixState.DROPPED_STATE;
import static com.linkedin.venice.helix.HelixState.ERROR_STATE;
import static com.linkedin.venice.helix.HelixState.LEADER_STATE;
import static com.linkedin.venice.helix.HelixState.OFFLINE_STATE;
import static com.linkedin.venice.helix.HelixState.STANDBY_STATE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_HELIX_FROM_STATE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_HELIX_STATE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_HELIX_TO_STATE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_THREAD_POOL_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceHelixSteadyState;
import com.linkedin.venice.utils.OpenTelemetryDataTestUtils;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.metrics.MetricsRepository;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ParticipantStateTransitionStatsOtelTest {
  private static final String TEST_METRIC_PREFIX = "server";
  private static final String TEST_POOL_NAME = "test_pool";

  private InMemoryMetricReader inMemoryMetricReader;
  private VeniceMetricsRepository metricsRepository;
  private ParticipantStateTransitionStats stats;
  private ThreadPoolExecutor executor;

  @BeforeMethod
  public void setUp() {
    inMemoryMetricReader = InMemoryMetricReader.create();
    metricsRepository = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX)
            .setMetricEntities(SERVER_METRIC_ENTITIES)
            .setEmitOtelMetrics(true)
            .setOtelAdditionalMetricsReader(inMemoryMetricReader)
            .build());
    executor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
    stats = new ParticipantStateTransitionStats(metricsRepository, executor, TEST_POOL_NAME);
  }

  @AfterMethod
  public void tearDown() {
    if (metricsRepository != null) {
      metricsRepository.close();
    }
    if (executor != null) {
      executor.shutdownNow();
    }
  }

  @Test
  public void testBlockedThreadCount() {
    stats.incrementThreadBlockedOnOfflineToDroppedTransitionCount();
    stats.incrementThreadBlockedOnOfflineToDroppedTransitionCount();
    stats.decrementThreadBlockedOnOfflineToDroppedTransitionCount();

    // UP_DOWN_COUNTER: cumulative value = +1 +1 -1 = 1
    OpenTelemetryDataTestUtils.validateLongPointDataFromCounter(
        inMemoryMetricReader,
        1,
        buildTransitionAttributes(OFFLINE_STATE, DROPPED_STATE),
        BLOCKED_THREAD_COUNT.getMetricEntity().getMetricName(),
        TEST_METRIC_PREFIX);
  }

  @Test
  public void testInProgressTransitionCount() {
    stats.trackStateTransitionStarted(OFFLINE_STATE, STANDBY_STATE);
    stats.trackStateTransitionStarted(OFFLINE_STATE, STANDBY_STATE);

    // UP_DOWN_COUNTER: 2 starts = +2
    OpenTelemetryDataTestUtils.validateLongPointDataFromCounter(
        inMemoryMetricReader,
        2,
        buildTransitionAttributes(OFFLINE_STATE, STANDBY_STATE),
        IN_PROGRESS_COUNT.getMetricEntity().getMetricName(),
        TEST_METRIC_PREFIX);

    stats.trackStateTransitionCompleted(OFFLINE_STATE, STANDBY_STATE);

    // UP_DOWN_COUNTER: +2 -1 = 1
    OpenTelemetryDataTestUtils.validateLongPointDataFromCounter(
        inMemoryMetricReader,
        1,
        buildTransitionAttributes(OFFLINE_STATE, STANDBY_STATE),
        IN_PROGRESS_COUNT.getMetricEntity().getMetricName(),
        TEST_METRIC_PREFIX);
  }

  @Test
  public void testSteadyStateCount() {
    // Complete a transition to STANDBY — steady state count should increment
    stats.trackStateTransitionStarted(OFFLINE_STATE, STANDBY_STATE);
    stats.trackStateTransitionCompleted(OFFLINE_STATE, STANDBY_STATE);

    // UP_DOWN_COUNTER: +1 for STANDBY
    OpenTelemetryDataTestUtils.validateLongPointDataFromCounter(
        inMemoryMetricReader,
        1,
        buildSteadyStateAttributes(STANDBY_STATE),
        STEADY_STATE_COUNT.getMetricEntity().getMetricName(),
        TEST_METRIC_PREFIX);

    // Start transition from STANDBY to LEADER — STANDBY count decrements
    stats.trackStateTransitionStarted(STANDBY_STATE, LEADER_STATE);
    stats.trackStateTransitionCompleted(STANDBY_STATE, LEADER_STATE);

    // UP_DOWN_COUNTER: STANDBY = +1 -1 = 0, LEADER = +1
    OpenTelemetryDataTestUtils.validateLongPointDataFromCounter(
        inMemoryMetricReader,
        0,
        buildSteadyStateAttributes(STANDBY_STATE),
        STEADY_STATE_COUNT.getMetricEntity().getMetricName(),
        TEST_METRIC_PREFIX);

    OpenTelemetryDataTestUtils.validateLongPointDataFromCounter(
        inMemoryMetricReader,
        1,
        buildSteadyStateAttributes(LEADER_STATE),
        STEADY_STATE_COUNT.getMetricEntity().getMetricName(),
        TEST_METRIC_PREFIX);
  }

  @Test
  public void testErrorSteadyStateCount() {
    stats.trackStateTransitionStarted(OFFLINE_STATE, ERROR_STATE);
    stats.trackStateTransitionCompleted(OFFLINE_STATE, ERROR_STATE);

    OpenTelemetryDataTestUtils.validateLongPointDataFromCounter(
        inMemoryMetricReader,
        1,
        buildSteadyStateAttributes(ERROR_STATE),
        STEADY_STATE_COUNT.getMetricEntity().getMetricName(),
        TEST_METRIC_PREFIX);
  }

  @Test
  public void testVeniceHelixSteadyStateEnumMatchesEnabledSteadyStates() {
    // Guard: if someone adds/removes a state from ENABLED_STEADY_STATES, VeniceHelixSteadyState must be updated
    assertEquals(
        VeniceHelixSteadyState.values().length,
        ParticipantStateTransitionStats.ENABLED_STEADY_STATES.size(),
        "VeniceHelixSteadyState enum values must match ENABLED_STEADY_STATES. "
            + "Update VeniceHelixSteadyState when changing ENABLED_STEADY_STATES.");
    for (VeniceHelixSteadyState state: VeniceHelixSteadyState.values()) {
      assertTrue(
          ParticipantStateTransitionStats.ENABLED_STEADY_STATES.contains(state.name()),
          "VeniceHelixSteadyState." + state.name() + " is not in ENABLED_STEADY_STATES");
    }
  }

  @Test
  public void testNoNpeWhenOtelDisabled() {
    try (VeniceMetricsRepository disabledRepo = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX).setEmitOtelMetrics(false).build())) {
      exerciseAllRecordingPaths(disabledRepo);
    }
  }

  @Test
  public void testNoNpeWhenPlainMetricsRepository() {
    exerciseAllRecordingPaths(new MetricsRepository());
  }

  private void exerciseAllRecordingPaths(MetricsRepository repo) {
    ParticipantStateTransitionStats safeStats = new ParticipantStateTransitionStats(repo, executor, TEST_POOL_NAME);
    safeStats.incrementThreadBlockedOnOfflineToDroppedTransitionCount();
    safeStats.decrementThreadBlockedOnOfflineToDroppedTransitionCount();
    safeStats.trackStateTransitionStarted(OFFLINE_STATE, STANDBY_STATE);
    safeStats.trackStateTransitionCompleted(OFFLINE_STATE, STANDBY_STATE);
  }

  private static Attributes buildTransitionAttributes(String fromState, String toState) {
    return Attributes.builder()
        .put(VENICE_THREAD_POOL_NAME.getDimensionNameInDefaultFormat(), TEST_POOL_NAME)
        .put(VENICE_HELIX_FROM_STATE.getDimensionNameInDefaultFormat(), fromState.toLowerCase())
        .put(VENICE_HELIX_TO_STATE.getDimensionNameInDefaultFormat(), toState.toLowerCase())
        .build();
  }

  private static Attributes buildSteadyStateAttributes(String state) {
    return Attributes.builder()
        .put(VENICE_THREAD_POOL_NAME.getDimensionNameInDefaultFormat(), TEST_POOL_NAME)
        .put(VENICE_HELIX_STATE.getDimensionNameInDefaultFormat(), state.toLowerCase())
        .build();
  }
}
