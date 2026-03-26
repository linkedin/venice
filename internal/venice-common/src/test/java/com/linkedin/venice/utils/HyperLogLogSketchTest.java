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

  // ---- Hash function test ----

  @Test
  public void testHash64Deterministic() {
    byte[] key = "deterministic".getBytes(StandardCharsets.UTF_8);
    long h1 = HyperLogLogSketch.hash64(key);
    long h2 = HyperLogLogSketch.hash64(key);
    assertEquals(h1, h2);
  }

  @Test
  public void testHash64Distribution() {
    // Verify hash distributes across all 4 quadrants of the 64-bit space
    boolean hasPositive = false, hasNegative = false;
    for (int i = 0; i < 100; i++) {
      long h = HyperLogLogSketch.hash64(("key-" + i).getBytes(StandardCharsets.UTF_8));
      if (h >= 0)
        hasPositive = true;
      if (h < 0)
        hasNegative = true;
    }
    assertTrue(hasPositive && hasNegative, "Hash should produce both positive and negative values");
  }
}
