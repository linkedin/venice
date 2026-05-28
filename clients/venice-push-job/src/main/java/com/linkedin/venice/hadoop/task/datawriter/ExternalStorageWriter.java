package com.linkedin.venice.hadoop.task.datawriter;

import com.linkedin.venice.utils.VeniceProperties;
import java.io.Closeable;
import java.util.List;


/**
 * VPJ-side dual-write sink. The Venice Push Job invokes implementations of this interface alongside Kafka
 * produce so that each record lands both in Venice and in a configured external storage system.
 *
 * <p>Lifecycle (one instance per Spark/MR partition writer task):
 * <ol>
 *   <li>{@link #configure(VeniceProperties, String, int)} once, right after no-arg construction.</li>
 *   <li>{@link #batchPut(List)} per buffered batch of records. The wrapper guarantees records inside a
 *       single batch are in the same order as they were submitted; ordering across batches is preserved as
 *       well.</li>
 *   <li>{@link #flush()} once, before {@link #close()}, to force durability of any buffered records.</li>
 *   <li>{@link #close()} once.</li>
 * </ol>
 *
 * <p>Implementations are loaded reflectively by class name from the VPJ config key
 * {@code push.job.external.storage.writer.class}. Must have a public no-arg constructor. Must be idempotent
 * on key — Spark task retries can replay a partition.
 *
 * <p>{@code batchPut} is <em>synchronous from the caller's point of view</em>: it must throw on durable-write
 * failure so the Spark task fails and is retried. Implementations may buffer internally as long as
 * {@link #flush()} drains.
 *
 * <p><b>Deletes are not part of this SPI.</b> Dual-write to external storage targets batch-only stores from
 * clean batch-push input, where every record carries a non-null value and no delete is ever produced
 * (Venice's batch-push path only emits deletes for KIF repush with TTL tombstones, which is out of scope for
 * dual-write). The {@link DualWriteVeniceWriter} that wraps this SPI throws
 * {@link UnsupportedOperationException} from its {@code delete} overrides so a stray delete fails the Spark
 * task loudly rather than silently leaving the external sink in a divergent state.
 */
public interface ExternalStorageWriter extends Closeable {
  /**
   * Called once on the executor after no-arg construction. Implementations should read whatever job-level
   * configuration they need from {@code jobProps} and prepare any per-task resources (connection pools,
   * output paths, etc.).
   */
  void configure(VeniceProperties jobProps, String topicName, int partitionId);

  /**
   * Durable bulk-write of {@code records}. The list may contain one or more records; an empty list is a
   * no-op. Implementations should treat this as a synchronous write — return only after every record is
   * durable, or throw.
   */
  void batchPut(List<ExternalStorageRecord> records);

  /** Force durability of any buffered records. Called by the partition writer before {@link #close()}. */
  void flush();
}
