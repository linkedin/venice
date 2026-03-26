package com.linkedin.venice.spark.datawriter.task;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import org.testng.annotations.Test;


public class HyperLogLogAccumulatorTest {
  @Test
  public void testEmptyHllReturnsZero() {
    HyperLogLogAccumulator hll = new HyperLogLogAccumulator();
    assertTrue(hll.isZero());
    assertEquals(hll.value().longValue(), 0L);
  }

  @Test
  public void testSingleKey() {
    HyperLogLogAccumulator hll = new HyperLogLogAccumulator();
    hll.add("key1".getBytes(StandardCharsets.UTF_8));
    assertFalse(hll.isZero());
    assertEquals(hll.value().longValue(), 1L);
  }

  @Test
  public void testDuplicateKeysDoNotInflateCardinality() {
    HyperLogLogAccumulator hll = new HyperLogLogAccumulator();
    byte[] key = "same-key".getBytes(StandardCharsets.UTF_8);
    for (int i = 0; i < 1000; i++) {
      hll.add(key);
    }
    assertEquals(hll.value().longValue(), 1L);
  }

  @Test
  public void testManyUniqueKeys() {
    HyperLogLogAccumulator hll = new HyperLogLogAccumulator();
    int n = 100_000;
    for (int i = 0; i < n; i++) {
      hll.add(("key-" + i).getBytes(StandardCharsets.UTF_8));
    }
    long estimate = hll.value();
    double relativeError = Math.abs((double) (estimate - n)) / n;
    // HLL with p=14 has ~0.8% standard error; allow 3% for safety
    assertTrue(relativeError < 0.03, "Relative error " + relativeError + " exceeds 3% for " + n + " unique keys");
  }

  @Test
  public void testMergeDisjointSets() {
    HyperLogLogAccumulator hll1 = new HyperLogLogAccumulator();
    HyperLogLogAccumulator hll2 = new HyperLogLogAccumulator();

    for (int i = 0; i < 10_000; i++) {
      hll1.add(("a-" + i).getBytes(StandardCharsets.UTF_8));
    }
    for (int i = 0; i < 10_000; i++) {
      hll2.add(("b-" + i).getBytes(StandardCharsets.UTF_8));
    }

    hll1.merge(hll2);
    long estimate = hll1.value();
    double relativeError = Math.abs((double) (estimate - 20_000)) / 20_000;
    assertTrue(relativeError < 0.03, "Merged estimate error " + relativeError + " exceeds 3%");
  }

  @Test
  public void testMergeWithOverlap() {
    HyperLogLogAccumulator hll1 = new HyperLogLogAccumulator();
    HyperLogLogAccumulator hll2 = new HyperLogLogAccumulator();

    // Both add keys 0-9999, hll2 also adds 10000-19999
    for (int i = 0; i < 10_000; i++) {
      hll1.add(("key-" + i).getBytes(StandardCharsets.UTF_8));
      hll2.add(("key-" + i).getBytes(StandardCharsets.UTF_8));
    }
    for (int i = 10_000; i < 20_000; i++) {
      hll2.add(("key-" + i).getBytes(StandardCharsets.UTF_8));
    }

    hll1.merge(hll2);
    long estimate = hll1.value();
    double relativeError = Math.abs((double) (estimate - 20_000)) / 20_000;
    assertTrue(relativeError < 0.03, "Overlap merged estimate error " + relativeError + " exceeds 3%");
  }

  @Test
  public void testCopy() {
    HyperLogLogAccumulator hll = new HyperLogLogAccumulator();
    for (int i = 0; i < 1000; i++) {
      hll.add(("key-" + i).getBytes(StandardCharsets.UTF_8));
    }

    HyperLogLogAccumulator copy = (HyperLogLogAccumulator) hll.copy();
    assertEquals(copy.value(), hll.value());

    // Mutating copy should not affect original
    copy.add("extra-key".getBytes(StandardCharsets.UTF_8));
    // Values might still be equal due to HLL approximation, but registers should differ
    copy.reset();
    assertTrue(copy.isZero());
    assertFalse(hll.isZero());
  }

  @Test
  public void testReset() {
    HyperLogLogAccumulator hll = new HyperLogLogAccumulator();
    hll.add("key".getBytes(StandardCharsets.UTF_8));
    assertFalse(hll.isZero());

    hll.reset();
    assertTrue(hll.isZero());
    assertEquals(hll.value().longValue(), 0L);
  }

  @Test
  public void testNullAndEmptyKeyIgnored() {
    HyperLogLogAccumulator hll = new HyperLogLogAccumulator();
    hll.add(null);
    hll.add(new byte[0]);
    assertTrue(hll.isZero());
    assertEquals(hll.value().longValue(), 0L);
  }
}
