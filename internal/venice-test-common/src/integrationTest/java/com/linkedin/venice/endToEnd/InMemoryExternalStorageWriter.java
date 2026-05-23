package com.linkedin.venice.endToEnd;

import com.linkedin.venice.hadoop.task.datawriter.ExternalStorageRecord;
import com.linkedin.venice.hadoop.task.datawriter.ExternalStorageWriter;
import com.linkedin.venice.utils.VeniceProperties;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Integration-test {@link ExternalStorageWriter} that captures every {@code batchPut} / {@code delete} into
 * static per-topic state. Each entry stores the raw {@code [4-byte BE schemaId][payload]} blob that the
 * wrapper passed in, so the verification side can prove the on-disk format matches Venice's RocksDB layout.
 *
 * <p>Spark runs in local mode in integration tests, so static state is shared between Spark task threads
 * and the test's verification code. {@link #clearAll()} resets everything between tests.
 */
public class InMemoryExternalStorageWriter implements ExternalStorageWriter {
  /** Per-topic, key → RocksDB-formatted value blob ({@code [schemaId][payload]}). */
  private static final ConcurrentMap<String, ConcurrentMap<ByteBuffer, byte[]>> SINK = new ConcurrentHashMap<>();

  /** Per-topic max observed batch size across all {@code batchPut} invocations on every task. */
  private static final ConcurrentMap<String, AtomicInteger> MAX_BATCH_SIZE_OBSERVED = new ConcurrentHashMap<>();

  /** Per-topic total number of {@code batchPut} invocations across all tasks. */
  private static final ConcurrentMap<String, AtomicInteger> BATCH_PUT_INVOCATIONS = new ConcurrentHashMap<>();

  /**
   * Key that the integration test uses to verify the OSS prefix-forwarding rule end-to-end: any
   * {@code push.job.external.storage.*} property set by the test on the driver must reach
   * {@link #configure}'s {@code VeniceProperties} on the executor.
   */
  static final String FORWARDED_CONFIG_PROBE_KEY = "push.job.external.storage.test.forwarded.value";

  /** Per-topic snapshot of {@link #FORWARDED_CONFIG_PROBE_KEY} captured at configure() time. */
  private static final ConcurrentMap<String, String> FORWARDED_CONFIG_OBSERVED = new ConcurrentHashMap<>();

  private String topicName;
  private ConcurrentMap<ByteBuffer, byte[]> shard;

  public InMemoryExternalStorageWriter() {
  }

  @Override
  public void configure(VeniceProperties jobProps, String topicName, int partitionId) {
    this.topicName = topicName;
    this.shard = SINK.computeIfAbsent(topicName, k -> new ConcurrentHashMap<>());
    // Capture the probe key so the integration test can verify that arbitrary keys under the
    // push.job.external.storage.* prefix are propagated from VPJ driver props to the executor.
    String observed = jobProps.getString(FORWARDED_CONFIG_PROBE_KEY, "");
    if (!observed.isEmpty()) {
      FORWARDED_CONFIG_OBSERVED.put(topicName, observed);
    }
  }

  @Override
  public void batchPut(List<ExternalStorageRecord> records) {
    if (records.isEmpty()) {
      return;
    }
    BATCH_PUT_INVOCATIONS.computeIfAbsent(topicName, k -> new AtomicInteger()).incrementAndGet();
    MAX_BATCH_SIZE_OBSERVED.computeIfAbsent(topicName, k -> new AtomicInteger())
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

  /** Snapshot of everything written to the sink for the given topic. Empty if nothing has been written. */
  public static Map<ByteBuffer, byte[]> snapshotForTopic(String topicName) {
    return SINK.getOrDefault(topicName, new ConcurrentHashMap<>());
  }

  /** Largest single-call {@code batchPut} size seen for the given topic. {@code 0} if no batchPut yet. */
  public static int maxBatchSizeFor(String topicName) {
    AtomicInteger counter = MAX_BATCH_SIZE_OBSERVED.get(topicName);
    return counter == null ? 0 : counter.get();
  }

  /** Total number of {@code batchPut} invocations seen for the given topic. {@code 0} if none. */
  public static int batchPutInvocationsFor(String topicName) {
    AtomicInteger counter = BATCH_PUT_INVOCATIONS.get(topicName);
    return counter == null ? 0 : counter.get();
  }

  /**
   * Value of {@link #FORWARDED_CONFIG_PROBE_KEY} seen by {@code configure()} for the given topic.
   * {@code null} if no instance observed it (either because nothing was configured for that topic or
   * because the prefix-forwarding rule failed to propagate the key to the executor).
   */
  public static String forwardedConfigObservedFor(String topicName) {
    return FORWARDED_CONFIG_OBSERVED.get(topicName);
  }

  /** Reset all captured state. Call from test @AfterMethod / @AfterClass to avoid cross-test leakage. */
  public static void clearAll() {
    SINK.clear();
    MAX_BATCH_SIZE_OBSERVED.clear();
    BATCH_PUT_INVOCATIONS.clear();
    FORWARDED_CONFIG_OBSERVED.clear();
  }
}
