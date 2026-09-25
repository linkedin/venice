package com.linkedin.venice.stats.routing;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.exceptions.VeniceException;
import java.util.Random;
import java.util.function.DoubleSupplier;
import java.util.function.IntToDoubleFunction;
import org.testng.annotations.Test;


/**
 * Focused tests for {@link LatencyAdaptiveGroupSelector}, in particular the {@code excludedGroupId} path (used by the
 * fast client on retries) and the edge cases. The router's full behavioural math is covered separately by the router's
 * strategy test, which now delegates to this selector.
 */
public class LatencyAdaptiveGroupSelectorTest {
  private static IntToDoubleFunction latencies(double... byGroup) {
    return g -> byGroup[g];
  }

  /** A random supplier just below 1.0 makes the reservoir draw keep the first scanned candidate (deterministic). */
  private static final DoubleSupplier PICK_FIRST = () -> Math.nextDown(1.0);

  @Test
  public void testInvalidKnobsThrow() {
    IntToDoubleFunction lat = latencies(10.0, 10.0);
    // evenUntil == fullSkew (both in ms)
    assertThrows(VeniceException.class, () -> new LatencyAdaptiveGroupSelector(lat, 15.0, 15.0, 1.0, PICK_FIRST));
    // evenUntil > fullSkew
    assertThrows(VeniceException.class, () -> new LatencyAdaptiveGroupSelector(lat, 30.0, 10.0, 1.0, PICK_FIRST));
    // evenUntil < 0
    assertThrows(VeniceException.class, () -> new LatencyAdaptiveGroupSelector(lat, -1.0, 30.0, 1.0, PICK_FIRST));
    // interpolation exponent == 0
    assertThrows(VeniceException.class, () -> new LatencyAdaptiveGroupSelector(lat, 10.0, 30.0, 0.0, PICK_FIRST));
    // interpolation exponent < 0
    assertThrows(VeniceException.class, () -> new LatencyAdaptiveGroupSelector(lat, 10.0, 30.0, -1.0, PICK_FIRST));
  }

  @Test
  public void testInvalidGroupCountThrows() {
    LatencyAdaptiveGroupSelector selector = new LatencyAdaptiveGroupSelector(latencies(10.0));
    assertThrows(VeniceException.class, () -> selector.selectGroup(0, 0));
    assertThrows(VeniceException.class, () -> selector.selectGroup(-1, 0));
    assertThrows(
        VeniceException.class,
        () -> selector.selectGroup(LatencyAdaptiveGroupSelector.MAX_ALLOWED_GROUP + 1, 0));
  }

  @Test
  public void testSingleGroupAlwaysSelected() {
    LatencyAdaptiveGroupSelector selector = new LatencyAdaptiveGroupSelector(latencies(10.0));
    assertEquals(selector.selectGroup(1, 0), 0);
    assertEquals(selector.selectGroup(1, 0, -1), 0);
  }

  @Test
  public void testUnmeasuredGroupsStayEven() {
    // No group measured yet (latency <= 0) => skew 0 => even. With PICK_FIRST the first scanned group (the scan start)
    // is chosen, so the choice tracks startGroupId.
    LatencyAdaptiveGroupSelector selector =
        new LatencyAdaptiveGroupSelector(latencies(-1.0, -1.0, -1.0), 10.0, 30.0, 1.0, PICK_FIRST);
    assertEquals(selector.selectGroup(3, 0), 0);
    assertEquals(selector.selectGroup(3, 1), 1);
    assertEquals(selector.selectGroup(3, 2), 2);
  }

  @Test
  public void testExcludedGroupNeverSelected() {
    // Exclude group 1 on every call, across a full sweep of random values and every scan start.
    double[] randoms = { 0.0, 0.1, 0.25, 0.5, 0.75, 0.9, 0.99, Math.nextDown(1.0) };
    int[] cursor = { 0 };
    DoubleSupplier cycling = () -> randoms[(cursor[0]++) % randoms.length];
    LatencyAdaptiveGroupSelector selector =
        new LatencyAdaptiveGroupSelector(latencies(5.0, 5.0, 5.0), 10.0, 30.0, 1.0, cycling);
    for (long requestId = 0; requestId < 60; requestId++) {
      int start = (int) (requestId % 3);
      int picked = selector.selectGroup(3, start, 1);
      assertNotEquals(picked, 1, "excluded group 1 must never be selected");
      assertTrue(picked == 0 || picked == 2, "picked must be a non-excluded group, got " + picked);
    }
  }

  @Test
  public void testExcludingTheOnlyGroupFallsBack() {
    // groupCount 1 and excluding group 0: the request must still be routed somewhere rather than dropped or looped.
    LatencyAdaptiveGroupSelector selector = new LatencyAdaptiveGroupSelector(latencies(10.0));
    assertEquals(selector.selectGroup(1, 0, 0), 0);
  }

  @Test
  public void testExcludedSlowGroupDoesNotDistortSkew() {
    // group0 = 5ms, group1 = 5ms, group2 = 500ms. Without exclusion the 500ms group forces full skew off group2.
    // Excluding group2 leaves two groups well under the 10ms even-until threshold => skew 0 => even split between 0
    // and 1, and group2 never appears.
    Random random = new Random(11);
    LatencyAdaptiveGroupSelector selector =
        new LatencyAdaptiveGroupSelector(latencies(5.0, 5.0, 500.0), 10.0, 30.0, 1.0, random::nextDouble);
    int group0 = 0;
    int group1 = 0;
    int total = 6000;
    for (int i = 0; i < total; i++) {
      int picked = selector.selectGroup(3, i % 3, 2);
      assertNotEquals(picked, 2);
      if (picked == 0) {
        group0++;
      } else {
        group1++;
      }
    }
    assertTrue(group0 > total * 0.4 && group1 > total * 0.4, "even split expected, got " + group0 + "/" + group1);
  }

  @Test
  public void testFullSkewFavoursFastGroup() {
    // group0 = 10ms, group1 = 100ms => slowest 100ms >= full-skew 30ms => full skew. strength(0)=1/10,
    // strength(1)=1/100, so the fast group should take roughly 10/11 of the traffic. Fixed-seed RNG keeps this
    // deterministic.
    Random random = new Random(7);
    LatencyAdaptiveGroupSelector selector =
        new LatencyAdaptiveGroupSelector(latencies(10.0, 100.0), 10.0, 30.0, 1.0, random::nextDouble);
    int fast = 0;
    int total = 8000;
    for (int i = 0; i < total; i++) {
      if (selector.selectGroup(2, i % 2) == 0) {
        fast++;
      }
    }
    assertTrue(fast > total * 0.8, "fast group should take the large majority at full skew, got " + fast + "/" + total);
  }

  @Test
  public void testStaysEvenBelowSlaThreshold() {
    // group0 = 5ms, group1 = 8ms. The 8ms slowest is under the 10ms even-until threshold, so despite the 1.6x spread
    // routing stays even - the absolute-ms threshold suppresses skew on sub-SLA noise.
    Random random = new Random(3);
    LatencyAdaptiveGroupSelector selector =
        new LatencyAdaptiveGroupSelector(latencies(5.0, 8.0), 10.0, 30.0, 1.0, random::nextDouble);
    int group0 = 0;
    int total = 8000;
    for (int i = 0; i < total; i++) {
      if (selector.selectGroup(2, i % 2) == 0) {
        group0++;
      }
    }
    assertTrue(
        group0 > total * 0.4 && group0 < total * 0.6,
        "even split expected below SLA, got " + group0 + "/" + total);
  }

  @Test
  public void testAllGroupsSlowButEqualStaysEven() {
    // All groups at 50ms, well above the 30ms full-skew threshold => skew 1. But equal latency means equal strength,
    // so full skew still resolves to an even split: there is no faster group to steer toward.
    Random random = new Random(5);
    LatencyAdaptiveGroupSelector selector =
        new LatencyAdaptiveGroupSelector(latencies(50.0, 50.0, 50.0), 10.0, 30.0, 1.0, random::nextDouble);
    int[] counts = new int[3];
    int total = 9000;
    for (int i = 0; i < total; i++) {
      counts[selector.selectGroup(3, i % 3)]++;
    }
    for (int g = 0; g < 3; g++) {
      assertTrue(counts[g] > total * 0.28, "group " + g + " should keep ~1/3 when all groups are equally slow");
    }
  }
}
