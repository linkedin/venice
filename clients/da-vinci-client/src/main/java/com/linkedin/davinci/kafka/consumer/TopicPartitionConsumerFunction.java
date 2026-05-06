package com.linkedin.davinci.kafka.consumer;

public interface TopicPartitionConsumerFunction {
  /**
   * Apply a pause/resume action on the given topic-partition.
   *
   * @return {@code true} if the action was applied to the underlying consumer; {@code false} if
   *     the implementation chose to no-op (e.g. because a higher-priority pause source — such as
   *     a store-level {@code IngestionPauseMode} — already owns the consumer state). Callers
   *     that maintain their own bookkeeping should gate updates on this value to avoid
   *     desynchronizing from real consumer state.
   */
  boolean execute(String topic, int partition);
}
