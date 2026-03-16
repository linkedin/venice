package com.linkedin.venice.writer;

/**
 * Callback hook invoked by {@link VeniceWriter} after size validation and before producing a record
 * to the pub-sub system. Not invoked for records rejected by size checks (RecordTooLargeException).
 * Set via {@link VeniceWriterOptions.Builder#setWriterHook(VeniceWriterHook)} at construction time.
 *
 * Threading: Called on the caller's thread (the thread invoking {@link VeniceWriter#put},
 * {@link VeniceWriter#delete}, or {@link VeniceWriter#update}) without holding partition locks.
 * Implementations must be thread-safe (multiple threads may call concurrently).
 *
 * Exception handling: Exceptions thrown from {@link #onBeforeProduce} will propagate up and
 * fail the write. Implementations should handle their own errors if they do not want to break
 * the write path.
 *
 * Chunking: For large records that get chunked, the hook fires once with the original
 * pre-chunking sizes, not per-chunk.
 */
public interface VeniceWriterHook {
  enum OperationType {
    PUT, DELETE, UPDATE
  }

  /**
   * Called with serialized byte sizes before producing to the pub-sub system.
   * May block to apply backpressure (e.g., quota throttling).
   *
   * @param operationType the type of write operation (PUT, DELETE, or UPDATE)
   * @param keySizeBytes serialized key size in bytes
   * @param valueSizeBytes serialized value/update-payload size in bytes (0 for deletes)
   */
  void onBeforeProduce(OperationType operationType, int keySizeBytes, int valueSizeBytes);
}
