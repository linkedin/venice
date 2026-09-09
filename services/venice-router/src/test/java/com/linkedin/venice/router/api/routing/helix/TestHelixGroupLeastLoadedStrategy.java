package com.linkedin.venice.router.api.routing.helix;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.alpini.base.concurrency.TimeoutProcessor;
import com.linkedin.venice.stats.routing.HelixGroupStats;
import io.tehuti.metrics.MetricsRepository;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestHelixGroupLeastLoadedStrategy {
  @Test
  public void testSelectGroup() {
    TimeoutProcessor timeoutProcessor = mock(TimeoutProcessor.class);
    doReturn(mock(TimeoutProcessor.TimeoutFuture.class)).when(timeoutProcessor).schedule(any(), anyLong(), any());
    HelixGroupLeastLoadedStrategy strategy =
        new HelixGroupLeastLoadedStrategy(timeoutProcessor, 10000, mock(HelixGroupStats.class));
    int groupNum = 3;
    // Group 0 is slow.
    Assert.assertEquals(strategy.selectGroup(0, groupNum), 0);
    Assert.assertEquals(strategy.selectGroup(1, groupNum), 1);
    Assert.assertEquals(strategy.selectGroup(2, groupNum), 2);
    strategy.finishRequest(1, 1, 1);
    strategy.finishRequest(2, 2, 1);
    Assert.assertEquals(strategy.selectGroup(3, groupNum), 1);
    Assert.assertEquals(strategy.selectGroup(4, groupNum), 2);
    strategy.finishRequest(0, 0, 1);
    strategy.finishRequest(3, 1, 1);
    strategy.finishRequest(4, 2, 1);
    // Group 0 is recovered
    Assert.assertEquals(strategy.selectGroup(5, groupNum), 2);
    Assert.assertEquals(strategy.selectGroup(6, groupNum), 0);
  }

  @Test
  public void testLatencyBasedGroupSelection() {
    TimeoutProcessor timeoutProcessor = mock(TimeoutProcessor.class);
    doReturn(mock(TimeoutProcessor.TimeoutFuture.class)).when(timeoutProcessor).schedule(any(), anyLong(), any());
    HelixGroupStats stats = new HelixGroupStats(new MetricsRepository());
    HelixGroupLeastLoadedStrategy strategy = new HelixGroupLeastLoadedStrategy(timeoutProcessor, 10000, stats);
    int groupNum = 3;
    Assert.assertEquals(strategy.selectGroup(0, groupNum), 0);
    Assert.assertEquals(strategy.selectGroup(1, groupNum), 1);
    Assert.assertEquals(strategy.selectGroup(2, groupNum), 2);
    // Group 2 is the fastest one
    strategy.finishRequest(0, 0, 2);
    strategy.finishRequest(1, 1, 3);
    strategy.finishRequest(2, 2, 1);
    Assert.assertEquals(strategy.selectGroup(3, groupNum), 2);
  }

  /**
   * A high-weight (many-key) request should make its assigned group appear proportionally more loaded, so
   * subsequent lighter requests are steered to the other groups until they catch up. This is the core of the
   * weight-aware balancing that prevents one Helix group from absorbing disproportionate RCU.
   */
  @Test
  public void testWeightedGroupSelectionSteersAwayFromHeavyGroup() {
    TimeoutProcessor timeoutProcessor = mock(TimeoutProcessor.class);
    doReturn(mock(TimeoutProcessor.TimeoutFuture.class)).when(timeoutProcessor).schedule(any(), anyLong(), any());
    HelixGroupLeastLoadedStrategy strategy =
        new HelixGroupLeastLoadedStrategy(timeoutProcessor, 10000, mock(HelixGroupStats.class));
    int groupNum = 3;
    // A single 10-key request lands on group 0, contributing 10 units of load.
    Assert.assertEquals(strategy.selectGroup(0, groupNum, 10), 0);
    // The next 9 single-key requests must all avoid group 0, since the other groups together can only reach
    // 9 units of load and each remains below group 0's 10.
    for (int requestId = 1; requestId <= 9; requestId++) {
      Assert.assertNotEquals(
          strategy.selectGroup(requestId, groupNum, 1),
          0,
          "Group 0 holds 10 units of weighted load and should be avoided by lighter requests");
    }
  }

  /**
   * Finishing a request must subtract exactly the weight that was added when the group was selected, so a heavy
   * request fully releases its load and the group becomes available again (decrement symmetry). Without storing
   * the per-request weight, a weighted increment paired with a unit decrement would leak load and permanently
   * skew selection.
   */
  @Test
  public void testWeightedDecrementSymmetry() {
    TimeoutProcessor timeoutProcessor = mock(TimeoutProcessor.class);
    doReturn(mock(TimeoutProcessor.TimeoutFuture.class)).when(timeoutProcessor).schedule(any(), anyLong(), any());
    HelixGroupLeastLoadedStrategy strategy =
        new HelixGroupLeastLoadedStrategy(timeoutProcessor, 10000, mock(HelixGroupStats.class));
    int groupNum = 3;
    // Heavy request on group 0, plus one unit request on each of the other two groups.
    Assert.assertEquals(strategy.selectGroup(0, groupNum, 10), 0);
    Assert.assertEquals(strategy.selectGroup(1, groupNum, 1), 1);
    Assert.assertEquals(strategy.selectGroup(2, groupNum, 1), 2);
    // Release the heavy request: its 10 units must be fully subtracted, leaving group 0 empty (0 units) while
    // groups 1 and 2 still hold 1 unit each.
    strategy.finishRequest(0, 0, 1);
    // The now-empty group 0 is the least loaded and must be selected next.
    Assert.assertEquals(strategy.selectGroup(100, groupNum, 1), 0);
  }

  /**
   * A zero or negative weight must be clamped to 1 so that a flood of zero-weight requests cannot all pile onto
   * a single group without moving its counter. Each request contributes at least one unit of load.
   */
  @Test
  public void testNonPositiveWeightClampedToOne() {
    TimeoutProcessor timeoutProcessor = mock(TimeoutProcessor.class);
    doReturn(mock(TimeoutProcessor.TimeoutFuture.class)).when(timeoutProcessor).schedule(any(), anyLong(), any());
    HelixGroupLeastLoadedStrategy strategy =
        new HelixGroupLeastLoadedStrategy(timeoutProcessor, 10000, mock(HelixGroupStats.class));
    int groupNum = 3;
    // A zero-weight request on group 0 must still add 1 unit of load.
    Assert.assertEquals(strategy.selectGroup(0, groupNum, 0), 0);
    // Because group 0 now holds 1 unit (not 0), a subsequent request that starts scanning at group 0 must skip
    // it in favor of an empty group. If the zero weight had NOT been clamped, group 0 would tie at 0 and win.
    Assert.assertEquals(strategy.selectGroup(3, groupNum, 1), 1);
    // Finishing the clamped request subtracts exactly 1 and must not drive the counter negative or throw.
    strategy.finishRequest(0, 0, 1);
    strategy.finishRequest(3, 1, 1);
    // Both groups are empty again; the next request starting at group 0 selects group 0.
    Assert.assertEquals(strategy.selectGroup(6, groupNum, 1), 0);
  }

  /**
   * The weight-aware overload must remain backward compatible: the two-argument {@code selectGroup} default and
   * an explicit weight of 1 must produce identical selection behavior (every request counts as exactly 1).
   */
  @Test
  public void testDefaultOverloadEquivalentToWeightOne() {
    TimeoutProcessor timeoutProcessor = mock(TimeoutProcessor.class);
    doReturn(mock(TimeoutProcessor.TimeoutFuture.class)).when(timeoutProcessor).schedule(any(), anyLong(), any());
    HelixGroupLeastLoadedStrategy strategy =
        new HelixGroupLeastLoadedStrategy(timeoutProcessor, 10000, mock(HelixGroupStats.class));
    int groupNum = 3;
    // Two-arg default overload (weight defaults to 1).
    Assert.assertEquals(strategy.selectGroup(0, groupNum), 0);
    // Explicit weight of 1 on the next group.
    Assert.assertEquals(strategy.selectGroup(1, groupNum, 1), 1);
    Assert.assertEquals(strategy.selectGroup(2, groupNum), 2);
    // With all groups holding exactly 1 unit, the next request starting at group 0 picks group 0.
    Assert.assertEquals(strategy.selectGroup(3, groupNum, 1), 0);
  }
}
