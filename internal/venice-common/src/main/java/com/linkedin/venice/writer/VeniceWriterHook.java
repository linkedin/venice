package com.linkedin.venice.writer;

/**
 * Callback hook invoked by {@link VeniceWriter} before producing each record to the pub-sub system.
 * Set via {@link VeniceWriterOptions.Builder#setWriterHook(VeniceWriterHook)} at construction time.
 *
 * Threading: Called from writer threads without holding partition locks.
 * Implementations must be thread-safe (multiple partitions may call concurrently).
 *
 * Exception handling: Exceptions thrown from {@link #onBeforeProduce} will propagate up and
 * fail the write. Implementations should handle their own errors if they do not want to break
 * the write path.
 *
 * Chunking: For large records that get chunked, the hook fires once with the original
 * pre-chunking sizes, not per-chunk.
 */
public interface VeniceWriterHook {
  /**
   * Called with serialized byte sizes before producing to the pub-sub system.
   * May block to apply backpressure (e.g., quota throttling).
   *
   * @param keySizeBytes serialized key size in bytes
   * @param valueSizeBytes serialized value/update-payload size in bytes (0 for deletes)
   */
  void onBeforeProduce(int keySizeBytes, int valueSizeBytes);
}
