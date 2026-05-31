package com.linkedin.venice.hadoop.task.datawriter;

import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.writer.AbstractVeniceWriter;
import com.linkedin.venice.writer.DeleteMetadata;
import com.linkedin.venice.writer.PutMetadata;
import com.linkedin.venice.writer.VeniceWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Wraps a Kafka-backed {@link AbstractVeniceWriter} and one or more {@link ExternalStorageWriter}s (one per
 * {@code DUAL_WRITE} target region) so each batch-push record is written to every external sink first and
 * then produced to Kafka. The Kafka future is what the caller waits on for at-least-once semantics;
 * external-sink durability is synchronous from the caller's point of view, so a failed external write throws
 * before the Kafka produce starts and the Spark task is retried.
 *
 * <p>When the push targets multiple regions, the same record is fanned out to every regional writer before
 * the Kafka produce. The guarantee is that <em>no Kafka produce happens for a batch until every regional
 * external write for that batch has succeeded</em>: if any regional write fails (after its bounded retry) the
 * exception propagates before the produce and fails the Spark task. Note this is not an all-or-nothing write
 * across the external sinks within a failed attempt — an earlier region in the fan-out may already hold the
 * batch while a later one failed. Consistency is restored by the whole-partition retry: external writes are
 * idempotent on key, so the next attempt overwrites the partially-written region rather than leaving it
 * divergent.
 *
 * <p><b>Region fan-out is sequential.</b> Each region's {@code batchPut} (including its bounded retries and
 * backoff) completes before the next region's begins, on the single partition-writer task thread. This keeps
 * the failure semantics and ordering simple, but the per-batch external-write latency is the sum across
 * regions. TODO(future iteration): parallelize the per-region fan-out (e.g. a small per-task executor that
 * issues the regional {@code batchPut}s concurrently and then joins, aggregating failures) when cross-region
 * write latency dominates the push.
 *
 * <p>The writer buffers up to {@code batchSize} consecutive put records and flushes them as a single
 * {@link ExternalStorageWriter#batchPut(List)} (per regional writer) before the corresponding Kafka produces
 * fire. {@code batchSize = 1} (the default) disables buffering: every record is forwarded immediately as a
 * one-element batch, matching pre-batching semantics. {@link #flush} and {@link #close} drain any pending
 * buffer before doing their own work, so the producer's record ordering is preserved.
 *
 * <p>{@code update} and {@code delete} are not supported — batch pushes from clean input never call either.
 * Both throw {@link UnsupportedOperationException} so a stray invocation fails the Spark task loudly rather
 * than silently leaving the external sink and Venice in divergent states.
 */
public class DualWriteVeniceWriter extends AbstractVeniceWriter<byte[], byte[], byte[]> {
  private static final Logger LOGGER = LogManager.getLogger(DualWriteVeniceWriter.class);

  private final AbstractVeniceWriter<byte[], byte[], byte[]> kafkaWriter;
  private final List<ExternalStorageWriter> externalWriters;
  private final int batchSize;
  private final int batchPutRetries;
  private final long batchPutRetryBackoffMs;
  private final List<BufferedPut> putBuffer;

  /**
   * Convenience constructor for callers that don't need the buffered retry policy (e.g. unit tests that
   * exercise the wrapper's other behaviors). {@code batchPutRetries = 0} means one attempt, original
   * throw propagates immediately.
   */
  public DualWriteVeniceWriter(
      String topicName,
      AbstractVeniceWriter<byte[], byte[], byte[]> kafkaWriter,
      ExternalStorageWriter externalWriter,
      int batchSize) {
    this(topicName, kafkaWriter, Collections.singletonList(externalWriter), batchSize, 0, 0L);
  }

  /** Single-region convenience overload. */
  public DualWriteVeniceWriter(
      String topicName,
      AbstractVeniceWriter<byte[], byte[], byte[]> kafkaWriter,
      ExternalStorageWriter externalWriter,
      int batchSize,
      int batchPutRetries,
      long batchPutRetryBackoffMs) {
    this(
        topicName,
        kafkaWriter,
        Collections.singletonList(externalWriter),
        batchSize,
        batchPutRetries,
        batchPutRetryBackoffMs);
  }

  public DualWriteVeniceWriter(
      String topicName,
      AbstractVeniceWriter<byte[], byte[], byte[]> kafkaWriter,
      List<ExternalStorageWriter> externalWriters,
      int batchSize,
      int batchPutRetries,
      long batchPutRetryBackoffMs) {
    super(topicName);
    if (externalWriters == null || externalWriters.isEmpty()) {
      throw new IllegalArgumentException("externalWriters must be non-empty");
    }
    if (batchSize < 1) {
      throw new IllegalArgumentException("batchSize must be >= 1, got " + batchSize);
    }
    if (batchPutRetries < 0) {
      throw new IllegalArgumentException("batchPutRetries must be >= 0, got " + batchPutRetries);
    }
    if (batchPutRetryBackoffMs < 0) {
      throw new IllegalArgumentException("batchPutRetryBackoffMs must be >= 0, got " + batchPutRetryBackoffMs);
    }
    this.kafkaWriter = kafkaWriter;
    this.externalWriters = new ArrayList<>(externalWriters);
    this.batchSize = batchSize;
    this.batchPutRetries = batchPutRetries;
    this.batchPutRetryBackoffMs = batchPutRetryBackoffMs;
    this.putBuffer = new ArrayList<>(batchSize);
  }

  @Override
  public CompletableFuture<PubSubProduceResult> put(
      byte[] key,
      byte[] value,
      int valueSchemaId,
      PubSubProducerCallback callback) {
    return bufferPut(new BufferedPut(key, value, valueSchemaId, VeniceWriter.APP_DEFAULT_LOGICAL_TS, callback, null));
  }

  @Override
  public CompletableFuture<PubSubProduceResult> put(
      byte[] key,
      byte[] value,
      int valueSchemaId,
      long logicalTimestamp,
      PubSubProducerCallback callback) {
    return bufferPut(new BufferedPut(key, value, valueSchemaId, logicalTimestamp, callback, null));
  }

  @Override
  public CompletableFuture<PubSubProduceResult> put(
      byte[] key,
      byte[] value,
      int valueSchemaId,
      PubSubProducerCallback callback,
      PutMetadata putMetadata) {
    return bufferPut(
        new BufferedPut(key, value, valueSchemaId, VeniceWriter.APP_DEFAULT_LOGICAL_TS, callback, putMetadata));
  }

  @Override
  public CompletableFuture<PubSubProduceResult> put(
      byte[] key,
      byte[] value,
      int valueSchemaId,
      long logicalTimestamp,
      PubSubProducerCallback callback,
      PutMetadata putMetadata) {
    return bufferPut(new BufferedPut(key, value, valueSchemaId, logicalTimestamp, callback, putMetadata));
  }

  @Override
  public CompletableFuture<PubSubProduceResult> delete(byte[] key, PubSubProducerCallback callback) {
    throw new UnsupportedOperationException(
        getClass().getSimpleName() + " does not support delete — dual-write to external storage targets "
            + "batch-only stores from clean batch-push input, which never call delete()");
  }

  @Override
  public CompletableFuture<PubSubProduceResult> delete(
      byte[] key,
      long logicalTimestamp,
      PubSubProducerCallback callback) {
    throw new UnsupportedOperationException(
        getClass().getSimpleName() + " does not support delete — dual-write to external storage targets "
            + "batch-only stores from clean batch-push input, which never call delete()");
  }

  @Override
  public CompletableFuture<PubSubProduceResult> delete(
      byte[] key,
      PubSubProducerCallback callback,
      DeleteMetadata deleteMetadata) {
    throw new UnsupportedOperationException(
        getClass().getSimpleName() + " does not support delete — dual-write to external storage targets "
            + "batch-only stores from clean batch-push input, which never call delete()");
  }

  @Override
  public Future<PubSubProduceResult> update(
      byte[] key,
      byte[] update,
      int valueSchemaId,
      int derivedSchemaId,
      PubSubProducerCallback callback) {
    throw new UnsupportedOperationException(getClass().getSimpleName() + " does not support update");
  }

  @Override
  public CompletableFuture<PubSubProduceResult> update(
      byte[] key,
      byte[] update,
      int valueSchemaId,
      int derivedSchemaId,
      long logicalTimestamp,
      PubSubProducerCallback callback) {
    throw new UnsupportedOperationException(getClass().getSimpleName() + " does not support update");
  }

  @Override
  public void flush() {
    drainBuffer();
    for (ExternalStorageWriter externalWriter: externalWriters) {
      externalWriter.flush();
    }
    kafkaWriter.flush();
  }

  @Override
  public void close() throws IOException {
    close(true);
  }

  @Override
  public void close(boolean gracefulClose) throws IOException {
    IOException firstError = null;
    // Self-flush so close() alone is sufficient to meet the ExternalStorageWriter lifecycle contract:
    // drains the wrapper's buffer and forces every regional externalWriter.flush() + kafkaWriter.flush()
    // before any of them are closed. AbstractPartitionWriter.close() also calls flush() explicitly before
    // close(), but self-flushing here protects any caller that uses the wrapper in a different lifecycle.
    try {
      flush();
    } catch (RuntimeException e) {
      firstError = new IOException("Failed to flush before close", e);
    }
    for (ExternalStorageWriter externalWriter: externalWriters) {
      try {
        externalWriter.close();
      } catch (IOException | RuntimeException e) {
        // External impls are pluggable — an unchecked throw from one regional writer must not skip closing
        // the remaining regional writers or kafkaWriter.close() below, otherwise resources leak during task
        // shutdown.
        IOException wrapped = e instanceof IOException ? (IOException) e : new IOException(e);
        if (firstError == null) {
          firstError = wrapped;
        } else {
          firstError.addSuppressed(wrapped);
        }
      }
    }
    try {
      kafkaWriter.close(gracefulClose);
    } catch (IOException | RuntimeException e) {
      IOException wrapped = e instanceof IOException ? (IOException) e : new IOException(e);
      if (firstError == null) {
        firstError = wrapped;
      } else {
        firstError.addSuppressed(wrapped);
      }
    }
    if (firstError != null) {
      throw firstError;
    }
  }

  /** Visible for testing — current buffered-but-not-yet-flushed put count. */
  int getBufferedPutCount() {
    return putBuffer.size();
  }

  private CompletableFuture<PubSubProduceResult> bufferPut(BufferedPut record) {
    putBuffer.add(record);
    if (putBuffer.size() >= batchSize) {
      return drainBuffer();
    }
    // Partial batch — return the placeholder; it will be completed when drainBuffer() runs.
    return record.future;
  }

  /**
   * Writes the buffered puts to the external sink as one {@code batchPut}, then forwards each record to
   * the Kafka writer in order. Returns the future of the last buffered record so the on-threshold path of
   * {@link #bufferPut} can return it to the caller. Returns {@code null} when the buffer is empty.
   *
   * <p>The body is wrapped in {@code try/catch/finally} so that on any throw — from
   * {@code externalWriter.batchPut} OR from a synchronous failure inside {@code invokeKafkaPut} — every
   * buffered record's placeholder future is completed exceptionally and the buffer is cleared. Without
   * this, a failure would leave the buffer dirty; a subsequent {@code close()} (which self-flushes via
   * {@code flush() → drainBuffer()}) would replay the same records, contradicting the at-least-once-then-
   * retry contract.
   */
  private CompletableFuture<PubSubProduceResult> drainBuffer() {
    if (putBuffer.isEmpty()) {
      return null;
    }
    List<ExternalStorageRecord> externalRecords = new ArrayList<>(putBuffer.size());
    for (BufferedPut buffered: putBuffer) {
      externalRecords.add(new ExternalStorageRecord(buffered.key, toRocksDbFormattedValue(buffered)));
    }
    CompletableFuture<PubSubProduceResult> last = null;
    try {
      // Fan out the same batch to every regional external sink before any Kafka produce. A failure on any
      // region (after its bounded retry) propagates before any produce and fails the push. Earlier regions in
      // the fan-out may already hold the batch when a later one fails; that partial state is reconciled by the
      // idempotent whole-partition retry (see the class-level contract), not prevented here.
      for (ExternalStorageWriter externalWriter: externalWriters) {
        batchPutWithRetry(externalWriter, externalRecords);
      }
      for (BufferedPut buffered: putBuffer) {
        CompletableFuture<PubSubProduceResult> kafkaFuture = invokeKafkaPut(buffered);
        kafkaFuture.whenComplete((result, error) -> {
          if (error != null) {
            buffered.future.completeExceptionally(error);
          } else {
            buffered.future.complete(result);
          }
        });
        last = buffered.future;
      }
      return last;
    } catch (Throwable t) {
      // Complete every pending future exceptionally so callers awaiting them see the failure. CompletableFuture
      // is idempotent on complete/completeExceptionally — futures already linked to a Kafka future in the loop
      // above will be unaffected by this call.
      for (BufferedPut buffered: putBuffer) {
        buffered.future.completeExceptionally(t);
      }
      throw t;
    } finally {
      // Always clear the buffer so the next drainBuffer() (e.g. close() self-flushing after this failure)
      // does not replay the same records to the external sink.
      putBuffer.clear();
    }
  }

  /**
   * Invoke {@code externalWriter.batchPut(records)} with bounded retry. Up to {@code batchPutRetries + 1}
   * attempts total, with a {@code batchPutRetryBackoffMs} fixed sleep between attempts. The original
   * (first) failure is preserved as the thrown exception; later failures are attached via
   * {@code addSuppressed} so operators can see the full retry pattern in the executor log.
   *
   * <p>Interruption during the backoff sleep aborts the retry — the executor wants to shut down — and
   * surfaces the original failure with the {@link InterruptedException} suppressed.
   */
  private void batchPutWithRetry(ExternalStorageWriter externalWriter, List<ExternalStorageRecord> records) {
    int attempts = batchPutRetries + 1;
    RuntimeException lastError = null;
    for (int attempt = 1; attempt <= attempts; attempt++) {
      try {
        externalWriter.batchPut(records);
        if (attempt > 1) {
          LOGGER.info("externalWriter.batchPut succeeded on attempt {}/{}", attempt, attempts);
        }
        return;
      } catch (RuntimeException e) {
        if (lastError == null) {
          lastError = e;
        } else {
          lastError.addSuppressed(e);
        }
        if (attempt < attempts) {
          LOGGER.warn(
              "externalWriter.batchPut failed on attempt {}/{}; retrying after {}ms",
              attempt,
              attempts,
              batchPutRetryBackoffMs,
              e);
          if (batchPutRetryBackoffMs > 0) {
            try {
              Thread.sleep(batchPutRetryBackoffMs);
            } catch (InterruptedException ie) {
              Thread.currentThread().interrupt();
              lastError.addSuppressed(ie);
              throw lastError;
            }
          }
        }
      }
    }
    throw lastError;
  }

  /**
   * Build a value blob matching Venice's RocksDB on-disk format: 4-byte big-endian schema id followed by
   * the compressed Avro payload. A reader pulling from the external sink reassembles the same logical bytes
   * a Venice client sees, regardless of whether the value was chunked for Kafka transport.
   */
  private static byte[] toRocksDbFormattedValue(BufferedPut bp) {
    byte[] formatted = new byte[ExternalStorageRecord.SCHEMA_ID_PREFIX_LENGTH + bp.value.length];
    ByteUtils.writeInt(formatted, bp.valueSchemaId, 0);
    System.arraycopy(bp.value, 0, formatted, ExternalStorageRecord.SCHEMA_ID_PREFIX_LENGTH, bp.value.length);
    return formatted;
  }

  private CompletableFuture<PubSubProduceResult> invokeKafkaPut(BufferedPut bp) {
    boolean hasMetadata = bp.putMetadata != null;
    boolean hasTimestamp = bp.logicalTimestamp != VeniceWriter.APP_DEFAULT_LOGICAL_TS;
    if (hasMetadata && hasTimestamp) {
      return kafkaWriter.put(bp.key, bp.value, bp.valueSchemaId, bp.logicalTimestamp, bp.callback, bp.putMetadata);
    }
    if (hasMetadata) {
      return kafkaWriter.put(bp.key, bp.value, bp.valueSchemaId, bp.callback, bp.putMetadata);
    }
    if (hasTimestamp) {
      return kafkaWriter.put(bp.key, bp.value, bp.valueSchemaId, bp.logicalTimestamp, bp.callback);
    }
    return kafkaWriter.put(bp.key, bp.value, bp.valueSchemaId, bp.callback);
  }

  private static final class BufferedPut {
    final byte[] key;
    final byte[] value;
    final int valueSchemaId;
    final long logicalTimestamp;
    final PubSubProducerCallback callback;
    final PutMetadata putMetadata;
    final CompletableFuture<PubSubProduceResult> future;

    BufferedPut(
        byte[] key,
        byte[] value,
        int valueSchemaId,
        long logicalTimestamp,
        PubSubProducerCallback callback,
        PutMetadata putMetadata) {
      this.key = key;
      this.value = value;
      this.valueSchemaId = valueSchemaId;
      this.logicalTimestamp = logicalTimestamp;
      this.callback = callback;
      this.putMetadata = putMetadata;
      this.future = new CompletableFuture<>();
    }
  }
}
