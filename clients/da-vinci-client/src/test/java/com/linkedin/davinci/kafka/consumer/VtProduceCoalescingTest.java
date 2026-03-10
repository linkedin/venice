package com.linkedin.davinci.kafka.consumer;

import static org.testng.Assert.assertEquals;

import com.linkedin.davinci.utils.ByteArrayKey;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.testng.annotations.Test;


/**
 * Tests for time-window-based VT produce coalescing logic. The coalescing decision is:
 * skip VT produce if the key was produced within the configured window (coalescingWindowMs).
 *
 * This tests the algorithm in isolation using the same data structures as
 * {@link PartitionConsumptionState#getVtCoalescingLastProduceTimeMs(byte[])} /
 * {@link PartitionConsumptionState#setVtCoalescingLastProduceTimeMs(byte[], long)}.
 */
public class VtProduceCoalescingTest {
  /**
   * When the window is 0 ms, every record after the first should be coalesced (window never expires
   * within the same millisecond wall-clock tick, but the first produce is always allowed).
   */
  @Test
  public void testZeroWindowCoalescesAllButFirst() {
    long coalescingWindowMs = 0;
    ConcurrentMap<ByteArrayKey, Long> lastProduceTimeMs = new ConcurrentHashMap<>();
    byte[] key = "keyA".getBytes();

    // First record: no prior produce → should produce
    CoalescingResult r1 = shouldCoalesce(lastProduceTimeMs, key, coalescingWindowMs, 1000);
    assertEquals(r1, CoalescingResult.PRODUCE, "First record for a key should always produce");

    // Second record at same time: within window → coalesced
    CoalescingResult r2 = shouldCoalesce(lastProduceTimeMs, key, coalescingWindowMs, 1000);
    assertEquals(r2, CoalescingResult.COALESCED, "Same-ms record should be coalesced with window=0");

    // Third record 1ms later: window=0 means 0ms window, so (1001-1000)=1 >= 0 → produce
    CoalescingResult r3 = shouldCoalesce(lastProduceTimeMs, key, coalescingWindowMs, 1001);
    assertEquals(r3, CoalescingResult.PRODUCE, "Record 1ms later should produce with window=0");
  }

  /**
   * With a 60-second window, records within the window are coalesced.
   */
  @Test
  public void testTimeWindowCoalescing() {
    long coalescingWindowMs = 60_000;
    ConcurrentMap<ByteArrayKey, Long> lastProduceTimeMs = new ConcurrentHashMap<>();
    byte[] key = "hotKey".getBytes();

    // t=0: first produce
    CoalescingResult r1 = shouldCoalesce(lastProduceTimeMs, key, coalescingWindowMs, 0);
    assertEquals(r1, CoalescingResult.PRODUCE, "First record should produce");

    // t=30s: within window → coalesced
    CoalescingResult r2 = shouldCoalesce(lastProduceTimeMs, key, coalescingWindowMs, 30_000);
    assertEquals(r2, CoalescingResult.COALESCED, "Record within window should be coalesced");

    // t=59.999s: still within window → coalesced
    CoalescingResult r3 = shouldCoalesce(lastProduceTimeMs, key, coalescingWindowMs, 59_999);
    assertEquals(r3, CoalescingResult.COALESCED, "Record just before window expiry should be coalesced");

    // t=60s: at window boundary (60000-0 = 60000 <= 60000) → still coalesced
    CoalescingResult r4 = shouldCoalesce(lastProduceTimeMs, key, coalescingWindowMs, 60_000);
    assertEquals(r4, CoalescingResult.COALESCED, "Record at window boundary should be coalesced");

    // t=60.001s: window expired (60001-0 = 60001 > 60000) → produce
    CoalescingResult r5 = shouldCoalesce(lastProduceTimeMs, key, coalescingWindowMs, 60_001);
    assertEquals(r5, CoalescingResult.PRODUCE, "Record just after window expiry should produce");

    // t=90s: within new window (started at t=60.001s) → coalesced
    CoalescingResult r6 = shouldCoalesce(lastProduceTimeMs, key, coalescingWindowMs, 90_000);
    assertEquals(r6, CoalescingResult.COALESCED, "Record within new window should be coalesced");
  }

  /**
   * Different keys are tracked independently.
   */
  @Test
  public void testIndependentKeyTracking() {
    long coalescingWindowMs = 60_000;
    ConcurrentMap<ByteArrayKey, Long> lastProduceTimeMs = new ConcurrentHashMap<>();
    byte[] keyA = "keyA".getBytes();
    byte[] keyB = "keyB".getBytes();

    // Both keys produce on first occurrence
    assertEquals(shouldCoalesce(lastProduceTimeMs, keyA, coalescingWindowMs, 0), CoalescingResult.PRODUCE);
    assertEquals(shouldCoalesce(lastProduceTimeMs, keyB, coalescingWindowMs, 0), CoalescingResult.PRODUCE);

    // Both within window
    assertEquals(shouldCoalesce(lastProduceTimeMs, keyA, coalescingWindowMs, 30_000), CoalescingResult.COALESCED);
    assertEquals(shouldCoalesce(lastProduceTimeMs, keyB, coalescingWindowMs, 30_000), CoalescingResult.COALESCED);

    // keyA window expires (60001 - 0 = 60001 > 60000), keyB still in window
    assertEquals(shouldCoalesce(lastProduceTimeMs, keyA, coalescingWindowMs, 60_001), CoalescingResult.PRODUCE);
    assertEquals(shouldCoalesce(lastProduceTimeMs, keyB, coalescingWindowMs, 50_000), CoalescingResult.COALESCED);
  }

  /**
   * Simulates a hot key with many rapid updates — all but the first should be coalesced
   * within the window, then the first record after expiry should produce.
   */
  @Test
  public void testHotKeyBurstCoalescing() {
    long coalescingWindowMs = 60_000;
    ConcurrentMap<ByteArrayKey, Long> lastProduceTimeMs = new ConcurrentHashMap<>();
    byte[] hotKey = "hotKey".getBytes();
    int burstCount = 100;

    // First record produces
    assertEquals(shouldCoalesce(lastProduceTimeMs, hotKey, coalescingWindowMs, 0), CoalescingResult.PRODUCE);

    // Next 99 records within window — all coalesced
    int coalesced = 0;
    for (int i = 1; i < burstCount; i++) {
      if (shouldCoalesce(lastProduceTimeMs, hotKey, coalescingWindowMs, (long) i * 100) == CoalescingResult.COALESCED) {
        coalesced++;
      }
    }
    assertEquals(coalesced, burstCount - 1, "All burst records except first should be coalesced");

    // After window expires, next record produces
    assertEquals(
        shouldCoalesce(lastProduceTimeMs, hotKey, coalescingWindowMs, 60_001),
        CoalescingResult.PRODUCE,
        "First record after window expiry should produce");
  }

  /**
   * With a negative window (feature disabled), nothing is coalesced.
   */
  @Test
  public void testNegativeWindowDisablesCoalescing() {
    long coalescingWindowMs = -1;
    ConcurrentMap<ByteArrayKey, Long> lastProduceTimeMs = new ConcurrentHashMap<>();
    byte[] key = "key".getBytes();

    // All records should produce when feature is disabled
    for (int i = 0; i < 10; i++) {
      assertEquals(
          shouldCoalesce(lastProduceTimeMs, key, coalescingWindowMs, (long) i * 1000),
          CoalescingResult.PRODUCE,
          "Record " + i + " should produce when feature disabled");
    }
    // Map should remain empty when disabled
    assertEquals(lastProduceTimeMs.size(), 0, "No tracking should occur when disabled");
  }

  // ---- Helpers ----

  private enum CoalescingResult {
    PRODUCE, COALESCED
  }

  /**
   * Mirrors the coalescing logic in ActiveActiveStoreIngestionTask.processMessageAndMaybeProduceToKafka().
   * Uses an explicit currentTimeMs parameter instead of System.currentTimeMillis() for deterministic testing.
   */
  private CoalescingResult shouldCoalesce(
      ConcurrentMap<ByteArrayKey, Long> lastProduceTimeMs,
      byte[] keyBytes,
      long coalescingWindowMs,
      long currentTimeMs) {
    if (coalescingWindowMs < 0) {
      return CoalescingResult.PRODUCE;
    }

    ByteArrayKey key = ByteArrayKey.wrap(keyBytes);
    Long lastProduce = lastProduceTimeMs.get(key);
    if (lastProduce != null && (currentTimeMs - lastProduce) <= coalescingWindowMs) {
      return CoalescingResult.COALESCED;
    }
    // Window expired or first produce — record and proceed
    lastProduceTimeMs.put(key, currentTimeMs);
    return CoalescingResult.PRODUCE;
  }
}
