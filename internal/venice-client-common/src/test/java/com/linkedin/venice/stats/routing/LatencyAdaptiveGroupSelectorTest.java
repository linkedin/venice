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

  @Test
  public void testFormatWeightsRendersPerGroupLatencyAndNormalizedWeights() {
    // Slowest group (g2 at 18ms) sits inside the 10->30ms band, so the draw is skewed toward the faster groups.
    LatencyAdaptiveGroupSelector selector =
        new LatencyAdaptiveGroupSelector(latencies(12.0, 14.0, 18.0), 10.0, 30.0, 1.0, PICK_FIRST);
    selector.setLogContext("router");
    String line = selector.formatWeights(3, 1);

    assertTrue(line.contains("[router]"), "should include the log context: " + line);
    assertTrue(line.contains("skew="), "should include the skew: " + line);
    assertTrue(
        line.contains("evenUntilMs=10.0") && line.contains("fullSkewMs=30.0"),
        "should include thresholds: " + line);
    assertTrue(
        line.contains("g0 lat=12.0ms") && line.contains("g2 lat=18.0ms"),
        "should include per-group latency: " + line);
    // The fastest group must carry strictly more weight than the slowest once the skew engages.
    double w0 = parseWeight(line, "g0");
    double w2 = parseWeight(line, "g2");
    assertTrue(w0 > w2, "faster group g0 should outweigh slower g2: " + line);
    // Normalized weights across all groups should sum to ~100%.
    double sum = parseWeight(line, "g0") + parseWeight(line, "g1") + parseWeight(line, "g2");
    assertTrue(Math.abs(sum - 100.0) < 0.5, "normalized weights should sum to ~100%, got " + sum + ": " + line);
  }

  @Test
  public void testFormatWeightsMarksUnmeasuredGroup() {
    // g1 has no datapoint yet (latency <= 0) => rendered as n/a, not a bogus number.
    LatencyAdaptiveGroupSelector selector =
        new LatencyAdaptiveGroupSelector(latencies(12.0, -1.0), 10.0, 30.0, 1.0, PICK_FIRST);
    String line = selector.formatWeights(2, 1);
    assertTrue(line.contains("g1 lat=n/a"), "unmeasured group should render lat=n/a: " + line);
  }

  @Test
  public void testBucketKeyForKeyCountMapsToFloor() {
    LatencyAdaptiveGroupSelector selector = new LatencyAdaptiveGroupSelector(
        (g, b) -> -1.0,
        (g, b) -> 0L,
        buckets(1, 9, 6, 18, 21, 30),
        1.0 / 3.0,
        1.0,
        10L,
        PICK_FIRST);
    assertEquals(selector.bucketKeyForKeyCount(0), 1, "below the lowest bound clamps to the first bucket");
    assertEquals(selector.bucketKeyForKeyCount(1), 1);
    assertEquals(selector.bucketKeyForKeyCount(5), 1);
    assertEquals(selector.bucketKeyForKeyCount(6), 6);
    assertEquals(selector.bucketKeyForKeyCount(20), 6);
    assertEquals(selector.bucketKeyForKeyCount(21), 21);
    assertEquals(selector.bucketKeyForKeyCount(10_000), 21, "above the top bound stays in the terminal bucket");
  }

  @Test
  public void testBucketedConstructorValidation() {
    // Map must start at key count 1.
    assertThrows(
        VeniceException.class,
        () -> new LatencyAdaptiveGroupSelector(
            (g, b) -> -1.0,
            (g, b) -> 0L,
            buckets(2, 30),
            1.0 / 3.0,
            1.0,
            10L,
            PICK_FIRST));
    // evenUntilFraction must be in (0, 1).
    assertThrows(
        VeniceException.class,
        () -> new LatencyAdaptiveGroupSelector(
            (g, b) -> -1.0,
            (g, b) -> 0L,
            buckets(1, 30),
            1.0,
            1.0,
            10L,
            PICK_FIRST));
    // minSampleCount must be >= 0.
    assertThrows(
        VeniceException.class,
        () -> new LatencyAdaptiveGroupSelector(
            (g, b) -> -1.0,
            (g, b) -> 0L,
            buckets(1, 30),
            1.0 / 3.0,
            1.0,
            -1L,
            PICK_FIRST));
  }

  @Test
  public void testPerBucketThresholdIsolatesRequestSizes() {
    // Two buckets: small keys (1-99) with a tight 9ms full-skew, large keys (100+) with a loose 300ms full-skew
    // (even-until 100ms). The SAME per-group latencies are read differently per bucket: in the small bucket the 20ms
    // group trips full skew and loses traffic; in the large bucket both groups sit under the even-until threshold and
    // routing stays even. This is the size-isolation a single mixed average cannot provide.
    LatencyAdaptiveGroupSelector.GroupBucketLatencyProvider lat = (g, bucketKey) -> g == 0 ? 5.0 : 20.0;
    LatencyAdaptiveGroupSelector.GroupBucketSampleCountProvider cnt = (g, bucketKey) -> 1000L;
    Random random = new Random(13);
    LatencyAdaptiveGroupSelector selector =
        new LatencyAdaptiveGroupSelector(lat, cnt, buckets(1, 9, 100, 300), 1.0 / 3.0, 1.0, 10L, random::nextDouble);

    int smallToFast = 0;
    int largeToFast = 0;
    int total = 8000;
    for (int i = 0; i < total; i++) {
      if (selector.selectGroup(2, i % 2, -1, 10) == 0) {
        smallToFast++;
      }
      if (selector.selectGroup(2, i % 2, -1, 250) == 0) {
        largeToFast++;
      }
    }
    assertTrue(smallToFast > total * 0.7, "small-key bucket should skew toward the fast group, got " + smallToFast);
    assertTrue(
        largeToFast > total * 0.4 && largeToFast < total * 0.6,
        "large-key bucket should stay even below its threshold, got " + largeToFast);
  }

  @Test
  public void testMinSampleFloorFallsBackToEven() {
    // g1 looks very slow (100ms) but has only 2 samples in the bucket, below the floor of 10 => it is treated as not
    // yet measured, so the slowest measured group is g0 at 5ms (under even-until) and routing stays even rather than
    // reacting to a thin, noisy average.
    LatencyAdaptiveGroupSelector.GroupBucketLatencyProvider lat = (g, bucketKey) -> g == 0 ? 5.0 : 100.0;
    LatencyAdaptiveGroupSelector.GroupBucketSampleCountProvider cnt = (g, bucketKey) -> g == 0 ? 1000L : 2L;
    Random random = new Random(17);
    LatencyAdaptiveGroupSelector selector =
        new LatencyAdaptiveGroupSelector(lat, cnt, buckets(1, 30), 1.0 / 3.0, 1.0, 10L, random::nextDouble);

    int group0 = 0;
    int total = 8000;
    for (int i = 0; i < total; i++) {
      if (selector.selectGroup(2, i % 2, -1, 5) == 0) {
        group0++;
      }
    }
    assertTrue(
        group0 > total * 0.4 && group0 < total * 0.6,
        "thin bucket should fall back to even routing, got " + group0 + "/" + total);
  }

  /** Builds a {@code lowerBound -> fullSkewMs} bucket map from flattened pairs, as the shared retry parser would produce. */
  private static java.util.NavigableMap<Integer, Integer> buckets(int... lowerBoundThenFullSkew) {
    java.util.TreeMap<Integer, Integer> map = new java.util.TreeMap<>();
    for (int i = 0; i < lowerBoundThenFullSkew.length; i += 2) {
      map.put(lowerBoundThenFullSkew[i], lowerBoundThenFullSkew[i + 1]);
    }
    return map;
  }

  private static double parseWeight(String line, String groupToken) {
    int gIdx = line.indexOf(groupToken + " ");
    int wIdx = line.indexOf("w=", gIdx);
    int pct = line.indexOf('%', wIdx);
    return Double.parseDouble(line.substring(wIdx + 2, pct));
  }
}
