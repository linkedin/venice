package com.linkedin.venice.hadoop.task.datawriter;

import com.linkedin.venice.utils.VeniceProperties;
import java.io.Closeable;
import java.util.List;


/**
 * VPJ-side dual-write sink. The Venice Push Job invokes implementations of this interface alongside Kafka
 * produce so that each record lands both in Venice and in a configured external storage system.
 *
 * <p>Lifecycle (one instance per Spark/MR partition writer task <em>per target region</em>):
 * <ol>
 *   <li>{@link #configure(VeniceProperties, String, int, String)} once, right after no-arg construction.</li>
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
 * <p><b>Per-region fan-out:</b> when the push targets multiple {@code DUAL_WRITE} regions, the wrapper loads
 * one writer instance per region and passes that region's name to {@link #configure}. A region-aware impl
 * routes its writes to that region's external-storage endpoint. The region list comes from
 * {@code push.job.dual.write.target.regions}; per-region endpoints are impl-specific config under the
 * {@code push.job.external.storage.*} prefix.
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
   *
   * @deprecated since the introduction of per-region dual-write fan-out; implement
   *             {@link #configure(VeniceProperties, String, int, String)} instead so the impl can route to
   *             the correct regional endpoint. Retained as a {@code default} purely for source compatibility
   *             with existing single-endpoint implementations — the framework no longer calls this overload
   *             directly. The default throws so an impl that overrides neither overload fails loudly rather
   *             than silently writing nothing.
   */
  @Deprecated
  default void configure(VeniceProperties jobProps, String topicName, int partitionId) {
    throw new UnsupportedOperationException(
        getClass().getName() + " must implement configure(jobProps, topicName, partitionId, region)");
  }

  /**
   * Region-aware configure. Called once on the executor after no-arg construction with the name of the
   * region whose external-storage endpoint this instance should write to. Implementations should read
   * whatever job-level configuration they need from {@code jobProps} (per-region endpoints live under the
   * {@code push.job.external.storage.*} prefix) and prepare per-task resources.
   *
   * <p>The default delegates to the deprecated {@link #configure(VeniceProperties, String, int)} overload
   * (dropping {@code region}) so existing single-endpoint impls keep working unchanged.
   */
  default void configure(VeniceProperties jobProps, String topicName, int partitionId, String region) {
    configure(jobProps, topicName, partitionId);
  }

  /**
   * Durable bulk-write of {@code records}. The list may contain one or more records; an empty list is a
   * no-op. Implementations should treat this as a synchronous write — return only after every record is
   * durable, or throw.
   */
  void batchPut(List<ExternalStorageRecord> records);

  /** Force durability of any buffered records. Called by the partition writer before {@link #close()}. */
  void flush();
}
