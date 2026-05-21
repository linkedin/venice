package com.linkedin.venice.listener.profiler;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Locale;
import org.testng.annotations.Test;


public class KeyPartitionProfilerTest {
  private static final String STORE_NAME = "fooStore";
  private static final String STORE_VERSION = "fooStore_v3";

  /**
   * Construct a profiler whose Phase 1 (warm-up) is already complete, so {@link
   * KeyPartitionProfiler#record} immediately enters Phase 2 and the per-partition candidate Set
   * is populated. Uses {@code startTimeMs = now - 15s} with {@code durationMs = 30s}, so:
   *   warmupEnd = now - 15s + 30s/3 = now - 5s   (5s in the past — well past warm-up)
   *   expiresAt = now - 15s + 30s   = now + 15s  (15s in the future — not yet expired)
   */
  private static KeyPartitionProfiler profilerInPhase2(int partitionCount, int topK) {
    long now = System.currentTimeMillis();
    return new KeyPartitionProfiler(STORE_NAME, STORE_VERSION, now - 15_000, 30_000, partitionCount, topK);
  }

  @Test
  public void recordsExactPartitionCounts() {
    // Counters are updated in both phases — Phase 1 timing is fine for this assertion.
    KeyPartitionProfiler profiler =
        new KeyPartitionProfiler(STORE_NAME, STORE_VERSION, System.currentTimeMillis(), 10_000, 4, 10);
    profiler.record("k1".getBytes(StandardCharsets.UTF_8), 0);
    profiler.record("k2".getBytes(StandardCharsets.UTF_8), 1);
    profiler.record("k3".getBytes(StandardCharsets.UTF_8), 1);
    profiler.record("k4".getBytes(StandardCharsets.UTF_8), 1);

    String json = profiler.toJson();
    assertTrue(json.contains("\"totalRequests\":4"), json);
    assertTrue(json.contains("\"partitionId\":1,\"count\":3"), json);
    assertTrue(json.contains("\"partitionId\":0,\"count\":1"), json);
  }

  @Test
  public void identifiesHottestKeyInPartition() {
    // Top-K population requires Phase 2; use a profiler already past warm-up.
    KeyPartitionProfiler profiler = profilerInPhase2(4, 10);
    byte[] hot = "hot".getBytes();
    byte[] cold = "cold".getBytes();
    for (int i = 0; i < 1000; i++) {
      profiler.record(hot, 1);
    }
    for (int i = 0; i < 200; i++) {
      profiler.record(cold, 1);
    }

    String json = profiler.toJson();
    assertTrue(json.contains("\"topKeysByPartition\":{\"1\":["), json);
    int hotIdx = json.indexOf("\"estimatedCount\":1000,");
    int coldIdx = json.indexOf("\"estimatedCount\":200,");
    assertTrue(hotIdx > 0 && coldIdx > hotIdx, "hot key should rank first; json=" + json);
  }

  @Test
  public void expiredRecordsAreDropped() {
    // Started 10s ago with a 1s window — already expired at construction time.
    long longAgo = System.currentTimeMillis() - 10_000;
    KeyPartitionProfiler profiler = new KeyPartitionProfiler(STORE_NAME, STORE_VERSION, longAgo, 1_000, 4, 10);
    assertTrue(profiler.isExpired());
    profiler.record("late".getBytes(), 0);
    String json = profiler.toJson();
    assertTrue(json.contains("\"totalRequests\":0"), json);
  }

  @Test
  public void outOfRangePartitionIsIgnored() {
    KeyPartitionProfiler profiler =
        new KeyPartitionProfiler(STORE_NAME, STORE_VERSION, System.currentTimeMillis(), 10_000, 2, 10);
    profiler.record("k".getBytes(), 99);
    profiler.record("k".getBytes(), -1);
    String json = profiler.toJson();
    assertTrue(json.contains("\"totalRequests\":0"), json);
  }

  @Test
  public void skewFactorMatchesHandComputed() {
    // skewFactor is derived from partition counters (updated in both phases).
    KeyPartitionProfiler profiler =
        new KeyPartitionProfiler(STORE_NAME, STORE_VERSION, System.currentTimeMillis(), 10_000, 3, 10);
    // p0: 100 hits, p1: 0, p2: 0 → only p0 non-zero, so skewFactor = 100/100 = 1.0
    for (int i = 0; i < 100; i++) {
      profiler.record(("k" + i).getBytes(), 0);
    }
    assertTrue(profiler.toJson().contains("\"skewFactor\":1.000"), profiler.toJson());

    // p0: 100 hits, p1: 20 hits → avg = 60, max = 100, skewFactor ≈ 1.667
    KeyPartitionProfiler skewed =
        new KeyPartitionProfiler(STORE_NAME, STORE_VERSION, System.currentTimeMillis(), 10_000, 3, 10);
    for (int i = 0; i < 100; i++) {
      skewed.record(("k" + i).getBytes(), 0);
    }
    for (int i = 0; i < 20; i++) {
      skewed.record(("k" + i).getBytes(), 1);
    }
    assertTrue(skewed.toJson().contains("\"skewFactor\":1.667"), skewed.toJson());
  }

  @Test
  public void rawKeysAreNeverPresentInOutput() {
    // Need Phase 2 so the secret key crosses the natural top-K threshold and lands in
    // topKeysByPartition. Without Phase 2 the test would pass trivially (empty topKeys).
    KeyPartitionProfiler profiler = profilerInPhase2(2, 10);
    String secretKey = "highly-sensitive-key-PII";
    for (int i = 0; i < 50; i++) {
      profiler.record(secretKey.getBytes(StandardCharsets.UTF_8), 0);
    }
    String json = profiler.toJson();
    assertFalse(json.contains(secretKey), "raw key must not appear in output");
    assertTrue(json.contains("\"keyHash\":\""), "key hash must appear in output");
  }

  @Test
  public void zeroRequestsProducesValidJson() {
    KeyPartitionProfiler profiler =
        new KeyPartitionProfiler(STORE_NAME, STORE_VERSION, System.currentTimeMillis(), 10_000, 2, 10);
    String json = profiler.toJson();
    assertTrue(json.startsWith("{"), json);
    assertTrue(json.endsWith("}"), json);
    assertTrue(json.contains("\"totalRequests\":0"), json);
    assertTrue(json.contains("\"partitionDistribution\":[]"), json);
    assertTrue(json.contains("\"topKeysByPartition\":{}"), json);
  }

  @Test
  public void jsonDecimalSeparatorIsLocaleStable() {
    Locale previous = Locale.getDefault();
    Locale.setDefault(Locale.forLanguageTag("fr-FR"));
    try {
      KeyPartitionProfiler profiler =
          new KeyPartitionProfiler(STORE_NAME, STORE_VERSION, System.currentTimeMillis(), 10_000, 3, 10);
      for (int i = 0; i < 100; i++) {
        profiler.record(("k" + i).getBytes(), 0);
      }
      for (int i = 0; i < 20; i++) {
        profiler.record(("k" + i).getBytes(), 1);
      }
      String json = profiler.toJson();
      assertTrue(
          json.contains("\"skewFactor\":1.667"),
          "JSON must use '.' as decimal separator regardless of locale; got: " + json);
    } finally {
      Locale.setDefault(previous);
    }
  }

  @Test
  public void warmupPhaseSkipsTopKPopulation() {
    // With current startTime we're squarely in Phase 1 — no key should populate topKeys even
    // though we record heavily.
    KeyPartitionProfiler profiler =
        new KeyPartitionProfiler(STORE_NAME, STORE_VERSION, System.currentTimeMillis(), 60_000, 4, 10);
    byte[] hot = "hot".getBytes();
    for (int i = 0; i < 1000; i++) {
      profiler.record(hot, 1);
    }
    String json = profiler.toJson();
    assertTrue(json.contains("\"totalRequests\":1000"), json);
    // No partition's candidate set was populated → topKeysByPartition is empty.
    assertTrue(json.contains("\"topKeysByPartition\":{}"), json);
  }

  @Test
  public void getterAccessors() {
    KeyPartitionProfiler profiler =
        new KeyPartitionProfiler(STORE_NAME, STORE_VERSION, System.currentTimeMillis(), 10_000, 2, 10);
    assertEquals(profiler.getStoreName(), STORE_NAME);
    assertEquals(profiler.getStoreVersion(), STORE_VERSION);
  }
}
