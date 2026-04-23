package com.linkedin.venice.utils;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.testng.annotations.Test;


public class HyperLogLogSketchTest {
  @Test
  public void testEmptySketch() {
    HyperLogLogSketch hll = new HyperLogLogSketch();
    assertTrue(hll.isEmpty());
    assertEquals(hll.estimate(), 0L);
    assertEquals(hll.getPrecision(), 14);
    assertEquals(hll.getRegisterCount(), 16384);
  }

  @Test
  public void testSingleElement() {
    HyperLogLogSketch hll = new HyperLogLogSketch();
    hll.add("hello".getBytes(StandardCharsets.UTF_8));
    assertFalse(hll.isEmpty());
    assertEquals(hll.estimate(), 1L);
  }

  @Test
  public void testDuplicatesSuppressed() {
    HyperLogLogSketch hll = new HyperLogLogSketch();
    byte[] key = "same-key".getBytes(StandardCharsets.UTF_8);
    for (int i = 0; i < 10000; i++) {
      hll.add(key);
    }
    assertEquals(hll.estimate(), 1L);
  }

  @Test
  public void testAccuracyWith100kKeys() {
    HyperLogLogSketch hll = new HyperLogLogSketch();
    int n = 100_000;
    for (int i = 0; i < n; i++) {
      hll.add(("key-" + i).getBytes(StandardCharsets.UTF_8));
    }
    long estimate = hll.estimate();
    double relativeError = Math.abs((double) (estimate - n)) / n;
    assertTrue(relativeError < 0.03, "Relative error " + relativeError + " exceeds 3%");
  }

  @Test
  public void testMergeDisjoint() {
    HyperLogLogSketch a = new HyperLogLogSketch();
    HyperLogLogSketch b = new HyperLogLogSketch();
    for (int i = 0; i < 10_000; i++) {
      a.add(("a-" + i).getBytes(StandardCharsets.UTF_8));
      b.add(("b-" + i).getBytes(StandardCharsets.UTF_8));
    }
    a.merge(b);
    double relativeError = Math.abs((double) (a.estimate() - 20_000)) / 20_000;
    assertTrue(relativeError < 0.03, "Merged estimate error " + relativeError + " exceeds 3%");
  }

  @Test
  public void testMergeWithOverlap() {
    HyperLogLogSketch a = new HyperLogLogSketch();
    HyperLogLogSketch b = new HyperLogLogSketch();
    for (int i = 0; i < 10_000; i++) {
      a.add(("key-" + i).getBytes(StandardCharsets.UTF_8));
      b.add(("key-" + i).getBytes(StandardCharsets.UTF_8));
    }
    for (int i = 10_000; i < 20_000; i++) {
      b.add(("key-" + i).getBytes(StandardCharsets.UTF_8));
    }
    a.merge(b);
    double relativeError = Math.abs((double) (a.estimate() - 20_000)) / 20_000;
    assertTrue(relativeError < 0.03, "Overlap merged error " + relativeError + " exceeds 3%");
  }

  @Test
  public void testCopy() {
    HyperLogLogSketch hll = new HyperLogLogSketch();
    for (int i = 0; i < 1000; i++) {
      hll.add(("key-" + i).getBytes(StandardCharsets.UTF_8));
    }
    HyperLogLogSketch copy = hll.copy();
    assertEquals(copy.estimate(), hll.estimate());
    // Mutating copy should not affect original
    copy.reset();
    assertTrue(copy.isEmpty());
    assertFalse(hll.isEmpty());
  }

  @Test
  public void testReset() {
    HyperLogLogSketch hll = new HyperLogLogSketch();
    hll.add("key".getBytes(StandardCharsets.UTF_8));
    assertFalse(hll.isEmpty());
    hll.reset();
    assertTrue(hll.isEmpty());
    assertEquals(hll.estimate(), 0L);
  }

  @Test
  public void testNullAndEmptyIgnored() {
    HyperLogLogSketch hll = new HyperLogLogSketch();
    hll.add(null);
    hll.add(new byte[0]);
    assertTrue(hll.isEmpty());
  }

  @Test
  public void testAddHash() {
    HyperLogLogSketch hll1 = new HyperLogLogSketch();
    HyperLogLogSketch hll2 = new HyperLogLogSketch();
    byte[] key = "test-key".getBytes(StandardCharsets.UTF_8);
    hll1.add(key);
    hll2.addHash(HyperLogLogSketch.hash64(key));
    assertEquals(hll1.estimate(), hll2.estimate());
  }

  // ---- Serialization tests ----

  @Test
  public void testSerializationRoundTrip() {
    HyperLogLogSketch hll = new HyperLogLogSketch();
    for (int i = 0; i < 50_000; i++) {
      hll.add(("key-" + i).getBytes(StandardCharsets.UTF_8));
    }
    long originalEstimate = hll.estimate();

    byte[] bytes = hll.toBytes();
    assertEquals(bytes.length, 1 + 16384); // 1 byte precision + 2^14 registers
    assertEquals(bytes[0], (byte) 14); // precision

    HyperLogLogSketch restored = HyperLogLogSketch.fromBytes(bytes);
    assertEquals(restored.estimate(), originalEstimate);
    assertEquals(restored.getPrecision(), 14);
  }

  @Test
  public void testByteBufferRoundTrip() {
    HyperLogLogSketch hll = new HyperLogLogSketch();
    for (int i = 0; i < 10_000; i++) {
      hll.add(("key-" + i).getBytes(StandardCharsets.UTF_8));
    }
    long originalEstimate = hll.estimate();

    ByteBuffer buffer = hll.toByteBuffer();
    HyperLogLogSketch restored = HyperLogLogSketch.fromByteBuffer(buffer);
    assertEquals(restored.estimate(), originalEstimate);
  }

  @Test
  public void testEmptySketchSerializationRoundTrip() {
    HyperLogLogSketch hll = new HyperLogLogSketch();
    byte[] bytes = hll.toBytes();
    HyperLogLogSketch restored = HyperLogLogSketch.fromBytes(bytes);
    assertTrue(restored.isEmpty());
    assertEquals(restored.estimate(), 0L);
  }

  @Test
  public void testSerializationPreservesAfterMerge() {
    HyperLogLogSketch a = new HyperLogLogSketch();
    HyperLogLogSketch b = new HyperLogLogSketch();
    for (int i = 0; i < 5_000; i++) {
      a.add(("a-" + i).getBytes(StandardCharsets.UTF_8));
      b.add(("b-" + i).getBytes(StandardCharsets.UTF_8));
    }
    a.merge(b);
    long mergedEstimate = a.estimate();

    HyperLogLogSketch restored = HyperLogLogSketch.fromBytes(a.toBytes());
    assertEquals(restored.estimate(), mergedEstimate);
  }

  @Test
  public void testDeserializeFromPartialBuffer() {
    // Simulate reading from a ByteBuffer that has position > 0 (e.g., Avro bytes field)
    HyperLogLogSketch hll = new HyperLogLogSketch();
    hll.add("key".getBytes(StandardCharsets.UTF_8));
    byte[] rawBytes = hll.toBytes();

    // Wrap with offset
    byte[] padded = new byte[10 + rawBytes.length];
    System.arraycopy(rawBytes, 0, padded, 10, rawBytes.length);
    ByteBuffer buffer = ByteBuffer.wrap(padded, 10, rawBytes.length).slice();

    HyperLogLogSketch restored = HyperLogLogSketch.fromByteBuffer(buffer);
    assertEquals(restored.estimate(), hll.estimate());
  }

  // ---- Custom precision tests ----

  @Test
  public void testCustomPrecision() {
    HyperLogLogSketch hll = new HyperLogLogSketch(10); // 1024 registers
    assertEquals(hll.getPrecision(), 10);
    assertEquals(hll.getRegisterCount(), 1024);
    for (int i = 0; i < 10_000; i++) {
      hll.add(("key-" + i).getBytes(StandardCharsets.UTF_8));
    }
    // Lower precision = higher error, but should still be reasonable
    double relativeError = Math.abs((double) (hll.estimate() - 10_000)) / 10_000;
    assertTrue(relativeError < 0.10, "Relative error " + relativeError + " exceeds 10% for p=10");
  }

  @Test
  public void testCustomPrecisionSerializationRoundTrip() {
    HyperLogLogSketch hll = new HyperLogLogSketch(8); // 256 registers
    hll.add("key".getBytes(StandardCharsets.UTF_8));
    byte[] bytes = hll.toBytes();
    assertEquals(bytes.length, 1 + 256);
    assertEquals(bytes[0], (byte) 8);

    HyperLogLogSketch restored = HyperLogLogSketch.fromBytes(bytes);
    assertEquals(restored.getPrecision(), 8);
    assertEquals(restored.estimate(), hll.estimate());
  }

  // ---- Error handling ----

  @Test
  public void testInvalidPrecision() {
    assertThrows(IllegalArgumentException.class, () -> new HyperLogLogSketch(3));
    assertThrows(IllegalArgumentException.class, () -> new HyperLogLogSketch(19));
  }

  @Test
  public void testDeserializeInvalidBytes() {
    assertThrows(IllegalArgumentException.class, () -> HyperLogLogSketch.fromBytes(null));
    assertThrows(IllegalArgumentException.class, () -> HyperLogLogSketch.fromBytes(new byte[0]));
    assertThrows(IllegalArgumentException.class, () -> HyperLogLogSketch.fromBytes(new byte[] { 14 })); // too short
    assertThrows(IllegalArgumentException.class, () -> HyperLogLogSketch.fromBytes(new byte[] { 3, 0, 0 })); // bad
                                                                                                             // precision
  }

  @Test
  public void testMergeDifferentPrecisionThrows() {
    HyperLogLogSketch a = new HyperLogLogSketch(10);
    HyperLogLogSketch b = new HyperLogLogSketch(12);
    assertThrows(IllegalArgumentException.class, () -> a.merge(b));
  }

  // ---- Split-merge simulation tests (p=18 for near-zero error) ----
  // These simulate the real repush scenario where one partition's keys
  // are spread across multiple Spark tasks (splits).
  //
  // p=18 gives ~0.1% standard error. For small key sets (< 50), HLL
  // is exact due to linear counting correction. For larger sets, we
  // allow 0.5% tolerance — generous for p=18 but avoids test flakiness.

  private static final int MAX_P = HyperLogLogSketch.MAX_PRECISION; // p=18, 262144 registers, ~0.1% error

  private static void assertEstimateWithinTolerance(long estimate, long expected, double tolerance, String message) {
    if (expected == 0) {
      assertEquals(estimate, 0, message);
      return;
    }
    double relativeError = Math.abs((double) (estimate - expected)) / expected;
    assertTrue(
        relativeError <= tolerance,
        message + " | expected=" + expected + ", actual=" + estimate + ", error="
            + String.format("%.4f%%", relativeError * 100));
  }

  /** At p=18, tolerance is 0.5% — generous but deterministically passing. */
  private static final double P18_TOLERANCE = 0.005;

  /**
   * Scenario: 2 splits read the EXACT SAME keys (full overlap).
   * After merge, cardinality should equal the number of unique keys, not 2x.
   *
   * Split 0a: [k0, k1, k2, ..., k999]
   * Split 0b: [k0, k1, k2, ..., k999]  (identical keys)
   * Merged:   unique = 1000
   */
  @Test
  public void testMergeSplitsWithIdenticalKeys() {
    int numKeys = 1000;
    HyperLogLogSketch split0a = new HyperLogLogSketch(MAX_P);
    HyperLogLogSketch split0b = new HyperLogLogSketch(MAX_P);

    for (int i = 0; i < numKeys; i++) {
      byte[] key = ("key-" + i).getBytes(StandardCharsets.UTF_8);
      split0a.add(key);
      split0b.add(key);
    }

    split0a.merge(split0b);
    assertEstimateWithinTolerance(
        split0a.estimate(),
        numKeys,
        P18_TOLERANCE,
        "Merging identical key sets should not inflate cardinality");
  }

  /**
   * Scenario: 2 splits read COMPLETELY DISJOINT keys.
   *
   * Split 0a: [k0, k1, ..., k499]
   * Split 0b: [k500, k501, ..., k999]
   * Merged:   unique = 1000
   */
  @Test
  public void testMergeSplitsWithDisjointKeys() {
    int keysPerSplit = 500;
    HyperLogLogSketch split0a = new HyperLogLogSketch(MAX_P);
    HyperLogLogSketch split0b = new HyperLogLogSketch(MAX_P);

    for (int i = 0; i < keysPerSplit; i++) {
      split0a.add(("key-" + i).getBytes(StandardCharsets.UTF_8));
    }
    for (int i = keysPerSplit; i < (long) keysPerSplit * 2; i++) {
      split0b.add(("key-" + i).getBytes(StandardCharsets.UTF_8));
    }

    split0a.merge(split0b);
    assertEstimateWithinTolerance(
        split0a.estimate(),
        (long) keysPerSplit * 2,
        P18_TOLERANCE,
        "Merging disjoint splits should sum cardinalities");
  }

  /**
   * Scenario: 3 splits with PARTIAL OVERLAP.
   *
   * Split 0a: [k0, k1, k2, k3, k4]       (5 keys)
   * Split 0b: [k3, k4, k5, k6, k7]       (5 keys, 2 overlap with 0a)
   * Split 0c: [k0, k5, k8, k9]            (4 keys, 2 overlap with 0a+0b)
   * Merged:   unique = {k0..k9} = 10
   */
  @Test
  public void testMergeThreeSplitsWithPartialOverlap() {
    HyperLogLogSketch split0a = new HyperLogLogSketch(MAX_P);
    HyperLogLogSketch split0b = new HyperLogLogSketch(MAX_P);
    HyperLogLogSketch split0c = new HyperLogLogSketch(MAX_P);

    for (int i = 0; i <= 4; i++) {
      split0a.add(("key-" + i).getBytes(StandardCharsets.UTF_8));
    }
    for (int i = 3; i <= 7; i++) {
      split0b.add(("key-" + i).getBytes(StandardCharsets.UTF_8));
    }
    for (int i: new int[] { 0, 5, 8, 9 }) {
      split0c.add(("key-" + i).getBytes(StandardCharsets.UTF_8));
    }

    split0a.merge(split0b);
    split0a.merge(split0c);
    // Small key set (10 keys) — exact at p=18 due to linear counting
    assertEquals(split0a.estimate(), 10L, "3 partially overlapping splits should yield correct union cardinality");
  }

  /**
   * Scenario: Many splits (10), each reading the same key set + some unique ones.
   * Simulates aggressive splitting with high overlap.
   *
   * Common keys: [k0..k99]  (100 keys, present in ALL splits)
   * Per-split unique: [k_split_i_0..k_split_i_9]  (10 unique per split)
   * Total unique = 100 + 10*10 = 200
   */
  @Test
  public void testMergeManySplitsWithHighOverlap() {
    int numSplits = 10;
    int commonKeys = 100;
    int uniquePerSplit = 10;
    int expectedUnique = commonKeys + (numSplits * uniquePerSplit);

    HyperLogLogSketch[] splits = new HyperLogLogSketch[numSplits];
    for (int s = 0; s < numSplits; s++) {
      splits[s] = new HyperLogLogSketch(MAX_P);
      // Common keys in every split
      for (int i = 0; i < commonKeys; i++) {
        splits[s].add(("common-" + i).getBytes(StandardCharsets.UTF_8));
      }
      // Unique keys per split
      for (int i = 0; i < uniquePerSplit; i++) {
        splits[s].add(("split-" + s + "-unique-" + i).getBytes(StandardCharsets.UTF_8));
      }
    }

    // Merge all into splits[0]
    for (int s = 1; s < numSplits; s++) {
      splits[0].merge(splits[s]);
    }

    assertEstimateWithinTolerance(
        splits[0].estimate(),
        expectedUnique,
        P18_TOLERANCE,
        "Many splits with high overlap should yield correct union");
  }

  /**
   * Scenario: Single key added across 100 splits.
   * Cardinality must remain 1 after all merges.
   */
  @Test
  public void testMergeManySplitsSingleKey() {
    int numSplits = 100;
    byte[] key = "the-only-key".getBytes(StandardCharsets.UTF_8);

    HyperLogLogSketch merged = new HyperLogLogSketch(MAX_P);
    for (int s = 0; s < numSplits; s++) {
      HyperLogLogSketch split = new HyperLogLogSketch(MAX_P);
      split.add(key);
      merged.merge(split);
    }

    assertEquals(merged.estimate(), 1L, "Same key across 100 splits should still be 1 unique");
  }

  /**
   * Scenario: Merge order should not matter (commutativity + associativity).
   *
   * (A merge B) merge C == (C merge A) merge B == A merge (B merge C)
   */
  @Test
  public void testMergeOrderIndependence() {
    HyperLogLogSketch a = new HyperLogLogSketch(MAX_P);
    HyperLogLogSketch b = new HyperLogLogSketch(MAX_P);
    HyperLogLogSketch c = new HyperLogLogSketch(MAX_P);

    for (int i = 0; i < 100; i++) {
      a.add(("a-" + i).getBytes(StandardCharsets.UTF_8));
    }
    for (int i = 50; i < 200; i++) {
      b.add(("b-" + i).getBytes(StandardCharsets.UTF_8));
    }
    for (int i = 150; i < 300; i++) {
      c.add(("c-" + i).getBytes(StandardCharsets.UTF_8));
    }

    // Order 1: (A merge B) merge C
    HyperLogLogSketch order1 = a.copy();
    order1.merge(b.copy());
    order1.merge(c.copy());

    // Order 2: (C merge A) merge B
    HyperLogLogSketch order2 = c.copy();
    order2.merge(a.copy());
    order2.merge(b.copy());

    // Order 3: A merge (B merge C)
    HyperLogLogSketch bc = b.copy();
    bc.merge(c.copy());
    HyperLogLogSketch order3 = a.copy();
    order3.merge(bc);

    assertEquals(order1.estimate(), order2.estimate(), "Merge must be order-independent (1 vs 2)");
    assertEquals(order1.estimate(), order3.estimate(), "Merge must be order-independent (1 vs 3)");
  }

  /**
   * Scenario: Idempotency — merging the same sketch multiple times should not change the result.
   */
  @Test
  public void testMergeIdempotency() {
    HyperLogLogSketch hll = new HyperLogLogSketch(MAX_P);
    for (int i = 0; i < 500; i++) {
      hll.add(("key-" + i).getBytes(StandardCharsets.UTF_8));
    }
    long estimateBefore = hll.estimate();

    HyperLogLogSketch duplicate = hll.copy();
    hll.merge(duplicate);
    assertEquals(hll.estimate(), estimateBefore, "Merging a copy should not change estimate (idempotent)");

    // Merge again — still idempotent
    hll.merge(duplicate);
    hll.merge(duplicate);
    hll.merge(duplicate);
    assertEquals(hll.estimate(), estimateBefore, "Repeated self-merge must remain idempotent");
  }

  /**
   * Scenario: Realistic repush simulation.
   * Partition 0 has 50,000 unique keys split across 5 tasks.
   * Each task reads a contiguous range (like PubSubSplitPlanner splits by offset range).
   */
  @Test
  public void testRealisticRepushSplitSimulation() {
    int totalKeys = 50_000;
    int numSplits = 5;
    int keysPerSplit = totalKeys / numSplits;

    HyperLogLogSketch[] splits = new HyperLogLogSketch[numSplits];
    for (int s = 0; s < numSplits; s++) {
      splits[s] = new HyperLogLogSketch(MAX_P);
      int start = s * keysPerSplit;
      int end = start + keysPerSplit;
      for (int i = start; i < end; i++) {
        splits[s].add(("partition0-key-" + i).getBytes(StandardCharsets.UTF_8));
      }
    }

    // Merge all splits (simulating Spark driver accumulator merge)
    HyperLogLogSketch merged = splits[0];
    for (int s = 1; s < numSplits; s++) {
      merged.merge(splits[s]);
    }

    assertEstimateWithinTolerance(
        merged.estimate(),
        totalKeys,
        P18_TOLERANCE,
        "Repush simulation: merged splits should match total unique keys");
  }

  /**
   * Scenario: Splits with duplicates within each split (compaction scenario).
   * Source VT might have multiple versions of same key within one offset range.
   *
   * Split 0a: [k0, k0, k1, k1, k2, k2]  (3 unique, 6 records)
   * Split 0b: [k2, k3, k3, k4]           (3 unique, 4 records)
   * Merged:   unique = {k0, k1, k2, k3, k4} = 5
   */
  @Test
  public void testMergeSplitsWithWithinSplitDuplicates() {
    HyperLogLogSketch split0a = new HyperLogLogSketch(MAX_P);
    HyperLogLogSketch split0b = new HyperLogLogSketch(MAX_P);

    // Split 0a: each key added twice (simulates multiple versions in source VT)
    for (int i = 0; i < 3; i++) {
      byte[] key = ("key-" + i).getBytes(StandardCharsets.UTF_8);
      split0a.add(key);
      split0a.add(key); // duplicate within split
    }

    // Split 0b: overlap on k2, plus new keys with duplicates
    split0b.add("key-2".getBytes(StandardCharsets.UTF_8));
    split0b.add("key-3".getBytes(StandardCharsets.UTF_8));
    split0b.add("key-3".getBytes(StandardCharsets.UTF_8)); // duplicate
    split0b.add("key-4".getBytes(StandardCharsets.UTF_8));

    split0a.merge(split0b);
    // Small key set (5 keys) — exact at p=18 due to linear counting
    assertEquals(
        split0a.estimate(),
        5L,
        "Within-split duplicates + cross-split overlap should yield correct unique count");
  }

  /**
   * Scenario: Empty split merged with non-empty split.
   * One split had no records (empty partition range).
   */
  @Test
  public void testMergeWithEmptySplit() {
    HyperLogLogSketch nonEmpty = new HyperLogLogSketch(MAX_P);
    HyperLogLogSketch empty = new HyperLogLogSketch(MAX_P);

    for (int i = 0; i < 100; i++) {
      nonEmpty.add(("key-" + i).getBytes(StandardCharsets.UTF_8));
    }

    nonEmpty.merge(empty);
    // Small set (100 keys) — exact at p=18 due to linear counting
    assertEquals(nonEmpty.estimate(), 100L, "Merging empty split should not change estimate");

    // Also test empty.merge(nonEmpty)
    empty.merge(nonEmpty);
    assertEquals(empty.estimate(), 100L, "Empty merged with non-empty should equal non-empty");
  }

  /**
   * Large-scale exactness test: p=18 with 100,000 unique keys.
   * At p=18 (~0.1% std error), for 100K keys the estimate should be within ~100 of exact.
   */
  @Test
  public void testMaxPrecisionExactness() {
    int n = 100_000;
    HyperLogLogSketch hll = new HyperLogLogSketch(MAX_P);
    for (int i = 0; i < n; i++) {
      hll.add(("key-" + i).getBytes(StandardCharsets.UTF_8));
    }
    assertEstimateWithinTolerance(hll.estimate(), n, P18_TOLERANCE, "At p=18, 100K keys should be within 0.5%");
  }

  /**
   * Large-scale split-merge exactness: 100K keys split across 10 tasks at p=18.
   */
  @Test
  public void testMaxPrecisionSplitMergeExactness() {
    int totalKeys = 100_000;
    int numSplits = 10;
    int keysPerSplit = totalKeys / numSplits;

    HyperLogLogSketch merged = new HyperLogLogSketch(MAX_P);
    for (int s = 0; s < numSplits; s++) {
      HyperLogLogSketch split = new HyperLogLogSketch(MAX_P);
      for (int i = s * keysPerSplit; i < (s + 1) * keysPerSplit; i++) {
        split.add(("key-" + i).getBytes(StandardCharsets.UTF_8));
      }
      merged.merge(split);
    }

    assertEstimateWithinTolerance(
        merged.estimate(),
        totalKeys,
        P18_TOLERANCE,
        "Split-merge at p=18: 100K keys across 10 splits should be within 0.5%");
  }

  // ---- Hash function tests ----

  @Test
  public void testHash64Deterministic() {
    byte[] key = "deterministic".getBytes(StandardCharsets.UTF_8);
    long h1 = HyperLogLogSketch.hash64(key);
    long h2 = HyperLogLogSketch.hash64(key);
    assertEquals(h1, h2);
  }

  @Test
  public void testHash64Distribution() {
    // Verify hash distributes across all 4 quadrants of the 64-bit space (top 2 bits)
    boolean[] quadrants = new boolean[4];
    for (int i = 0; i < 100; i++) {
      long h = HyperLogLogSketch.hash64(("key-" + i).getBytes(StandardCharsets.UTF_8));
      int quadrant = (int) ((h >>> 62) & 0x3);
      quadrants[quadrant] = true;
    }
    assertTrue(
        quadrants[0] && quadrants[1] && quadrants[2] && quadrants[3],
        "Hash should produce values in all 4 high-order-bit quadrants");
  }

  @Test
  public void testHash64NullThrows() {
    assertThrows(IllegalArgumentException.class, () -> HyperLogLogSketch.hash64(null));
  }

  @Test
  public void testHash64QualityAt1MKeys() {
    int n = 1_000_000;
    HyperLogLogSketch hll = new HyperLogLogSketch();
    for (int i = 0; i < n; i++) {
      hll.add(("key-" + i).getBytes(StandardCharsets.UTF_8));
    }
    long estimate = hll.estimate();
    double relativeError = Math.abs((double) (estimate - n)) / n;
    assertTrue(relativeError < 0.02, "At 1M keys, relative error " + relativeError + " exceeds 2%");
  }

  @Test
  public void testMergeNullThrows() {
    HyperLogLogSketch hll = new HyperLogLogSketch();
    assertThrows(IllegalArgumentException.class, () -> hll.merge(null));
  }
}
