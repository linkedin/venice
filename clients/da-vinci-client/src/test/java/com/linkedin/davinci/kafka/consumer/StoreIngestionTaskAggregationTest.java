package com.linkedin.davinci.kafka.consumer;

import static com.linkedin.davinci.kafka.consumer.ActiveKeyCountTestUtils.doBatch;
import static com.linkedin.davinci.kafka.consumer.ActiveKeyCountTestUtils.freshPcs;
import static com.linkedin.venice.offsets.OffsetRecord.ACTIVE_KEY_COUNT_NOT_TRACKED;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;

import com.linkedin.venice.stats.dimensions.ReplicaType;
import java.util.Arrays;
import org.testng.annotations.Test;


/**
 * Unit tests for the per-replica-type aggregation methods on {@link StoreIngestionTask}:
 * {@code getActiveKeyCount(ReplicaType)}, {@code getEstimatedUniqueIngestedKeyCount(ReplicaType)},
 * and the strict-equality {@code matchesReplicaType} helper.
 *
 * <p>Uses mock SIT with {@code doCallRealMethod} for the methods under test and
 * {@code doReturn(pcsList).when(task).getPartitionConsumptionStates()} to inject the PCS view —
 * mirroring the pattern in {@code ActiveKeyCountScenarioTest.createMetricsContext}.
 *
 * <p>Strict-equality contract:
 * <ul>
 *   <li>{@code ReplicaType.LEADER} matches only {@link LeaderFollowerStateType#LEADER}</li>
 *   <li>{@code ReplicaType.FOLLOWER} matches only {@link LeaderFollowerStateType#STANDBY}</li>
 *   <li>Partitions in any IN_TRANSITION_* / PAUSE_TRANSITION_* state contribute to neither</li>
 * </ul>
 */
public class StoreIngestionTaskAggregationTest {

  // --- Active-key-count ---
  @Test
  public void testGetActiveKeyCountSumsByReplicaType() {
    PartitionConsumptionState leaderPcs = freshPcs(LeaderFollowerStateType.LEADER);
    doBatch(leaderPcs, 100);
    PartitionConsumptionState followerPcs = freshPcs(LeaderFollowerStateType.STANDBY);
    doBatch(followerPcs, 200);

    StoreIngestionTask task = mockSitWith(leaderPcs, followerPcs);
    assertEquals(task.getActiveKeyCount(ReplicaType.LEADER), 100L, "Leader partitions only");
    assertEquals(task.getActiveKeyCount(ReplicaType.FOLLOWER), 200L, "Follower partitions only");
  }

  @Test
  public void testGetActiveKeyCountSkipsUntrackedPartitions() {
    PartitionConsumptionState tracked = freshPcs(LeaderFollowerStateType.LEADER);
    doBatch(tracked, 50);
    // untracked: never call doBatch -> stays at ACTIVE_KEY_COUNT_NOT_TRACKED (-1)
    PartitionConsumptionState untracked = freshPcs(LeaderFollowerStateType.LEADER);

    StoreIngestionTask task = mockSitWith(tracked, untracked);
    assertEquals(task.getActiveKeyCount(ReplicaType.LEADER), 50L, "Sum only tracked partitions; skip -1");
  }

  @Test
  public void testGetActiveKeyCountReturnsNotTrackedWhenAllUntracked() {
    PartitionConsumptionState pcs1 = freshPcs(LeaderFollowerStateType.LEADER);
    PartitionConsumptionState pcs2 = freshPcs(LeaderFollowerStateType.STANDBY);

    StoreIngestionTask task = mockSitWith(pcs1, pcs2);
    assertEquals(
        task.getActiveKeyCount(ReplicaType.LEADER),
        ACTIVE_KEY_COUNT_NOT_TRACKED,
        "All-untracked LEADER partitions");
    assertEquals(
        task.getActiveKeyCount(ReplicaType.FOLLOWER),
        ACTIVE_KEY_COUNT_NOT_TRACKED,
        "All-untracked FOLLOWER partitions");
  }

  @Test
  public void testGetActiveKeyCountReturnsNotTrackedWhenNoMatchingPartitions() {
    // Only LEADER partitions exist; FOLLOWER aggregate has zero matches.
    PartitionConsumptionState leaderPcs = freshPcs(LeaderFollowerStateType.LEADER);
    doBatch(leaderPcs, 100);

    StoreIngestionTask task = mockSitWith(leaderPcs);
    assertEquals(task.getActiveKeyCount(ReplicaType.LEADER), 100L);
    assertEquals(
        task.getActiveKeyCount(ReplicaType.FOLLOWER),
        ACTIVE_KEY_COUNT_NOT_TRACKED,
        "No FOLLOWER partitions -> -1 sentinel");
  }

  @Test
  public void testGetActiveKeyCountWithEmptyPartitions() {
    StoreIngestionTask task = mockSitWith();
    assertEquals(task.getActiveKeyCount(ReplicaType.LEADER), ACTIVE_KEY_COUNT_NOT_TRACKED);
    assertEquals(task.getActiveKeyCount(ReplicaType.FOLLOWER), ACTIVE_KEY_COUNT_NOT_TRACKED);
  }

  @Test
  public void testGetActiveKeyCountIncludesZeroCount() {
    // An empty batch push initializes the count to 0. 0 is "tracked but empty", not "untracked".
    PartitionConsumptionState pcs = freshPcs(LeaderFollowerStateType.LEADER);
    doBatch(pcs, 0);

    StoreIngestionTask task = mockSitWith(pcs);
    assertEquals(task.getActiveKeyCount(ReplicaType.LEADER), 0L, "0 means tracked-but-empty");
  }

  @Test
  public void testGetActiveKeyCountExcludesInTransitionFromBoth() {
    // IN_TRANSITION_FROM_STANDBY_TO_LEADER contributes to NEITHER LEADER nor FOLLOWER under
    // strict-equality matchesReplicaType. The partition has a real count (50), but it's filtered
    // out because the state is neither LEADER nor STANDBY.
    PartitionConsumptionState inTransition = freshPcs(LeaderFollowerStateType.IN_TRANSITION_FROM_STANDBY_TO_LEADER);
    doBatch(inTransition, 50);

    StoreIngestionTask task = mockSitWith(inTransition);
    assertEquals(
        task.getActiveKeyCount(ReplicaType.LEADER),
        ACTIVE_KEY_COUNT_NOT_TRACKED,
        "IN_TRANSITION not counted as LEADER");
    assertEquals(
        task.getActiveKeyCount(ReplicaType.FOLLOWER),
        ACTIVE_KEY_COUNT_NOT_TRACKED,
        "IN_TRANSITION not counted as FOLLOWER");
  }

  @Test
  public void testGetActiveKeyCountExcludesPauseTransitionFromBoth() {
    PartitionConsumptionState paused = freshPcs(LeaderFollowerStateType.PAUSE_TRANSITION_FROM_STANDBY_TO_LEADER);
    doBatch(paused, 75);

    StoreIngestionTask task = mockSitWith(paused);
    assertEquals(task.getActiveKeyCount(ReplicaType.LEADER), ACTIVE_KEY_COUNT_NOT_TRACKED);
    assertEquals(task.getActiveKeyCount(ReplicaType.FOLLOWER), ACTIVE_KEY_COUNT_NOT_TRACKED);
  }

  @Test
  public void testGetActiveKeyCountNoArgSumsAllAcrossStates() {
    // The no-arg overload sums every partition regardless of replica type, including
    // IN_TRANSITION_* states which the per-replica-type overloads exclude.
    PartitionConsumptionState leaderPcs = freshPcs(LeaderFollowerStateType.LEADER);
    doBatch(leaderPcs, 100);
    PartitionConsumptionState followerPcs = freshPcs(LeaderFollowerStateType.STANDBY);
    doBatch(followerPcs, 200);
    PartitionConsumptionState inTransition = freshPcs(LeaderFollowerStateType.IN_TRANSITION_FROM_STANDBY_TO_LEADER);
    doBatch(inTransition, 50);

    StoreIngestionTask task = mockSitWith(leaderPcs, followerPcs, inTransition);
    assertEquals(task.getActiveKeyCount(), 350L, "No-arg sums every state including IN_TRANSITION");
  }

  @Test
  public void testLeaderPlusFollowerLessThanTotalWhenInTransitionPresent() {
    // Invariant: per-replica-type aggregates exclude in-transition partitions, so their sum is
    // strictly less than the no-arg total whenever an IN_TRANSITION partition contributes a
    // non-zero count. Encoding this so dashboards built on per-replica-type metrics stay honest.
    PartitionConsumptionState leaderPcs = freshPcs(LeaderFollowerStateType.LEADER);
    doBatch(leaderPcs, 100);
    PartitionConsumptionState followerPcs = freshPcs(LeaderFollowerStateType.STANDBY);
    doBatch(followerPcs, 200);
    PartitionConsumptionState inTransition = freshPcs(LeaderFollowerStateType.IN_TRANSITION_FROM_STANDBY_TO_LEADER);
    doBatch(inTransition, 50);

    StoreIngestionTask task = mockSitWith(leaderPcs, followerPcs, inTransition);
    long total = task.getActiveKeyCount();
    long leader = task.getActiveKeyCount(ReplicaType.LEADER);
    long follower = task.getActiveKeyCount(ReplicaType.FOLLOWER);
    assertEquals(leader + follower, 300L, "LEADER+FOLLOWER excludes IN_TRANSITION");
    assertEquals(total, 350L, "Total includes IN_TRANSITION");
    if (leader + follower >= total) {
      throw new AssertionError("LEADER+FOLLOWER must be strictly less than total when IN_TRANSITION present");
    }
  }

  // --- Unique-ingested-key-count ---

  @Test
  public void testGetEstimatedUniqueIngestedKeyCountByReplicaType() {
    // Per-PCS HLL counts: leader=30k, follower=12k. Aggregate via strict matchesReplicaType.
    PartitionConsumptionState leaderPcs = freshPcs(LeaderFollowerStateType.LEADER);
    putKeys(leaderPcs, 30_000);
    PartitionConsumptionState followerPcs = freshPcs(LeaderFollowerStateType.STANDBY);
    putKeys(followerPcs, 12_000);

    StoreIngestionTask task = mockSitWith(leaderPcs, followerPcs);
    long leaderEstimate = task.getEstimatedUniqueIngestedKeyCount(ReplicaType.LEADER);
    long followerEstimate = task.getEstimatedUniqueIngestedKeyCount(ReplicaType.FOLLOWER);
    // HLL is approximate; check within ±5% of the inserted count.
    assertWithinTolerance(leaderEstimate, 30_000L, "LEADER HLL estimate");
    assertWithinTolerance(followerEstimate, 12_000L, "FOLLOWER HLL estimate");
  }

  @Test
  public void testGetEstimatedUniqueIngestedKeyCountExcludesInTransitionFromBoth() {
    PartitionConsumptionState inTransition = freshPcs(LeaderFollowerStateType.IN_TRANSITION_FROM_STANDBY_TO_LEADER);
    putKeys(inTransition, 5_000);

    StoreIngestionTask task = mockSitWith(inTransition);
    assertEquals(task.getEstimatedUniqueIngestedKeyCount(ReplicaType.LEADER), 0L);
    assertEquals(task.getEstimatedUniqueIngestedKeyCount(ReplicaType.FOLLOWER), 0L);
  }

  @Test
  public void testGetEstimatedUniqueIngestedKeyCountNoArgSumsAll() {
    // The no-arg overload sums every partition regardless of replica type — used by the Tehuti
    // path via IngestionStatsUtils.getEstimatedUniqueIngestedKeyCount.
    PartitionConsumptionState leaderPcs = freshPcs(LeaderFollowerStateType.LEADER);
    putKeys(leaderPcs, 1_000);
    PartitionConsumptionState followerPcs = freshPcs(LeaderFollowerStateType.STANDBY);
    putKeys(followerPcs, 2_000);
    PartitionConsumptionState inTransition = freshPcs(LeaderFollowerStateType.IN_TRANSITION_FROM_STANDBY_TO_LEADER);
    putKeys(inTransition, 500);

    StoreIngestionTask task = mockSitWith(leaderPcs, followerPcs, inTransition);
    long total = task.getEstimatedUniqueIngestedKeyCount();
    // Total should approximate 3500 across all states (including IN_TRANSITION). HLL approximate.
    assertWithinTolerance(total, 3_500L, "no-arg sums every state");
  }

  // --- Helpers ---

  /**
   * Builds a mock {@link StoreIngestionTask} that delegates the public aggregation methods to
   * their real implementations while returning the supplied PCS list from
   * {@code getPartitionConsumptionStates()}.
   */
  private static StoreIngestionTask mockSitWith(PartitionConsumptionState... pcsList) {
    StoreIngestionTask task = mock(StoreIngestionTask.class);
    doReturn(Arrays.asList(pcsList)).when(task).getPartitionConsumptionStates();
    doCallRealMethod().when(task).getActiveKeyCount();
    doCallRealMethod().when(task).getActiveKeyCount(nullable(ReplicaType.class));
    doCallRealMethod().when(task).getEstimatedUniqueIngestedKeyCount();
    doCallRealMethod().when(task).getEstimatedUniqueIngestedKeyCount(nullable(ReplicaType.class));
    return task;
  }

  /** Inserts {@code count} unique keys into the PCS's HLL sketch. */
  private static void putKeys(PartitionConsumptionState pcs, int count) {
    pcs.initializeUniqueKeyCountHll();
    for (int i = 0; i < count; i++) {
      pcs.trackKeyIngested(("k" + i).getBytes());
    }
  }

  /** Asserts the HLL estimate is within ±5% of the expected count (HLL is approximate). */
  private static void assertWithinTolerance(long actual, long expected, String message) {
    long tolerance = Math.max(1L, expected / 20); // 5%
    long delta = Math.abs(actual - expected);
    if (delta > tolerance) {
      throw new AssertionError(message + ": expected " + expected + " ± " + tolerance + " but got " + actual);
    }
  }

}
