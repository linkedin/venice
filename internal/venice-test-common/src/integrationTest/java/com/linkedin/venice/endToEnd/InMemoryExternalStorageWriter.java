package com.linkedin.venice.endToEnd;

import com.linkedin.venice.hadoop.task.datawriter.ExternalStorageRecord;
import com.linkedin.venice.hadoop.task.datawriter.ExternalStorageWriter;
import com.linkedin.venice.utils.VeniceProperties;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Integration-test {@link ExternalStorageWriter} that captures every {@code batchPut} into static
 * per-region, per-topic state. Each entry stores the raw {@code [4-byte BE schemaId][payload]} blob that the
 * wrapper passed in, so the verification side can prove the on-disk format matches Venice's RocksDB layout.
 *
 * <p>The sink is keyed by {@code (region, topic)} so a multi-region test can prove that a {@code DUAL_WRITE}
 * region's sink is populated while an {@code INTERNAL} region's sink stays empty. Topic-only accessors
 * aggregate across regions so single-region tests need not know the region name.
 *
 * <p>Spark runs in local mode in integration tests, so static state is shared between Spark task threads
 * and the test's verification code. {@link #clearAll()} resets everything between tests.
 */
public class InMemoryExternalStorageWriter implements ExternalStorageWriter {
  /** region → topic → (key → RocksDB-formatted value blob {@code [schemaId][payload]}). */
  private static final ConcurrentMap<String, ConcurrentMap<String, ConcurrentMap<ByteBuffer, byte[]>>> SINK =
      new ConcurrentHashMap<>();

  /** region → topic → max observed batch size across all {@code batchPut} invocations on every task. */
  private static final ConcurrentMap<String, ConcurrentMap<String, AtomicInteger>> MAX_BATCH_SIZE_OBSERVED =
      new ConcurrentHashMap<>();

  /** region → topic → total number of {@code batchPut} invocations across all tasks. */
  private static final ConcurrentMap<String, ConcurrentMap<String, AtomicInteger>> BATCH_PUT_INVOCATIONS =
      new ConcurrentHashMap<>();

  /**
   * Key that the integration test uses to verify the OSS prefix-forwarding rule end-to-end: any
   * {@code push.job.external.storage.*} property set by the test on the driver must reach
   * {@link #configure}'s {@code VeniceProperties} on the executor.
   */
  static final String FORWARDED_CONFIG_PROBE_KEY = "push.job.external.storage.test.forwarded.value";

  /** region → topic → snapshot of {@link #FORWARDED_CONFIG_PROBE_KEY} captured at configure() time. */
  private static final ConcurrentMap<String, ConcurrentMap<String, String>> FORWARDED_CONFIG_OBSERVED =
      new ConcurrentHashMap<>();

  private String region;
  private String topicName;
  private ConcurrentMap<ByteBuffer, byte[]> shard;

  public InMemoryExternalStorageWriter() {
  }

  @Override
  public void configure(VeniceProperties jobProps, String topicName, int partitionId, String region) {
    this.region = region;
    this.topicName = topicName;
    this.shard = SINK.computeIfAbsent(region, r -> new ConcurrentHashMap<>())
        .computeIfAbsent(topicName, k -> new ConcurrentHashMap<>());
    // Capture the probe key so the integration test can verify that arbitrary keys under the
    // push.job.external.storage.* prefix are propagated from VPJ driver props to the executor.
    String observed = jobProps.getString(FORWARDED_CONFIG_PROBE_KEY, "");
    if (!observed.isEmpty()) {
      FORWARDED_CONFIG_OBSERVED.computeIfAbsent(region, r -> new ConcurrentHashMap<>()).put(topicName, observed);
    }
  }

  @Override
  public void batchPut(List<ExternalStorageRecord> records) {
    if (records.isEmpty()) {
      return;
    }
    BATCH_PUT_INVOCATIONS.computeIfAbsent(region, r -> new ConcurrentHashMap<>())
        .computeIfAbsent(topicName, k -> new AtomicInteger())
        .incrementAndGet();
    MAX_BATCH_SIZE_OBSERVED.computeIfAbsent(region, r -> new ConcurrentHashMap<>())
        .computeIfAbsent(topicName, k -> new AtomicInteger())
        .accumulateAndGet(records.size(), Math::max);
    for (ExternalStorageRecord record: records) {
      shard.put(ByteBuffer.wrap(record.getKey()), record.getValue());
    }
  }

  @Override
  public void flush() {
  }

  @Override
  public void close() {
  }

  /**
   * Snapshot of everything written to the sink for {@code (region, topic)}. Empty if nothing was written.
   * Returns a defensive copy so callers cannot mutate (or observe concurrent mutation of) the backing state.
   */
  public static Map<ByteBuffer, byte[]> snapshotForRegionAndTopic(String region, String topicName) {
    ConcurrentMap<String, ConcurrentMap<ByteBuffer, byte[]>> byTopic = SINK.get(region);
    if (byTopic == null || byTopic.get(topicName) == null) {
      return new HashMap<>();
    }
    return new HashMap<>(byTopic.get(topicName));
  }

  /** Aggregate snapshot for {@code topic} across all regions. Empty if nothing has been written. */
  public static Map<ByteBuffer, byte[]> snapshotForTopic(String topicName) {
    Map<ByteBuffer, byte[]> merged = new HashMap<>();
    for (ConcurrentMap<String, ConcurrentMap<ByteBuffer, byte[]>> byTopic: SINK.values()) {
      ConcurrentMap<ByteBuffer, byte[]> shard = byTopic.get(topicName);
      if (shard != null) {
        merged.putAll(shard);
      }
    }
    return merged;
  }

  /** Regions whose sink received at least one record for {@code topic}. */
  public static Set<String> regionsWithDataForTopic(String topicName) {
    Set<String> regions = new HashSet<>();
    for (Map.Entry<String, ConcurrentMap<String, ConcurrentMap<ByteBuffer, byte[]>>> entry: SINK.entrySet()) {
      ConcurrentMap<ByteBuffer, byte[]> shard = entry.getValue().get(topicName);
      if (shard != null && !shard.isEmpty()) {
        regions.add(entry.getKey());
      }
    }
    return regions;
  }

  /** Largest single-call {@code batchPut} size seen for {@code topic} across all regions. {@code 0} if none. */
  public static int maxBatchSizeFor(String topicName) {
    int max = 0;
    for (ConcurrentMap<String, AtomicInteger> byTopic: MAX_BATCH_SIZE_OBSERVED.values()) {
      AtomicInteger counter = byTopic.get(topicName);
      if (counter != null) {
        max = Math.max(max, counter.get());
      }
    }
    return max;
  }

  /** Total number of {@code batchPut} invocations seen for {@code topic} across all regions. {@code 0} if none. */
  public static int batchPutInvocationsFor(String topicName) {
    int total = 0;
    for (ConcurrentMap<String, AtomicInteger> byTopic: BATCH_PUT_INVOCATIONS.values()) {
      AtomicInteger counter = byTopic.get(topicName);
      if (counter != null) {
        total += counter.get();
      }
    }
    return total;
  }

  /**
   * Value of {@link #FORWARDED_CONFIG_PROBE_KEY} seen by {@code configure()} for {@code topic} in any region.
   * {@code null} if no instance observed it (either because nothing was configured for that topic or because
   * the prefix-forwarding rule failed to propagate the key to the executor).
   */
  public static String forwardedConfigObservedFor(String topicName) {
    for (ConcurrentMap<String, String> byTopic: FORWARDED_CONFIG_OBSERVED.values()) {
      String observed = byTopic.get(topicName);
      if (observed != null) {
        return observed;
      }
    }
    return null;
  }

  /** Reset all captured state. Call from test @AfterMethod / @AfterClass to avoid cross-test leakage. */
  public static void clearAll() {
    SINK.clear();
    MAX_BATCH_SIZE_OBSERVED.clear();
    BATCH_PUT_INVOCATIONS.clear();
    FORWARDED_CONFIG_OBSERVED.clear();
  }
}
