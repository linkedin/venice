package com.linkedin.venice.vpj.pubsub.input;

import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.vpj.pubsub.input.splitter.CappedSplitCountStrategy;
import com.linkedin.venice.vpj.pubsub.input.splitter.FixedRecordCountSplitStrategy;
import com.linkedin.venice.vpj.pubsub.input.splitter.PubSubTopicPartitionSplitStrategy;
import com.linkedin.venice.vpj.pubsub.input.splitter.SingleSplitPerPartitionStrategy;
import java.util.List;


/**
 * Defines the available strategies for splitting a {@link PubSubTopicPartition}
 * into one or more {@link PubSubPartitionSplit} instances.
 *
 * <p>Each enum constant encapsulates a specific implementation of
 * {@link PubSubTopicPartitionSplitStrategy}. This allows the enum itself
 * to serve as the strategy selector, removing the need for a separate
 * factory or switch statement.</p>
 *
 * <p>Usage example:</p>
 * <pre>{@code
 * List<PubSubPartitionSplit> splits =
 *     PartitionSplitStrategy.SINGLE_SPLIT_PER_PARTITION.split(request);
 * }</pre>
 */
public enum PartitionSplitStrategy implements PubSubTopicPartitionSplitStrategy {
  /**
   * Splits a {@link PubSubTopicPartition} into multiple splits,
   * each containing a fixed target number of records.
   */
  FIXED_RECORD_COUNT(new FixedRecordCountSplitStrategy()),

  /**
   * Splits a {@link PubSubTopicPartition} into multiple splits,
   * with the total number of splits capped at a configured maximum.
   * The size of each split is calculated by dividing the total record
   * count by this maximum.
   */
  CAPPED_SPLIT_COUNT(new CappedSplitCountStrategy()),

  /**
   * Produces exactly one split per {@link PubSubTopicPartition}.
   * This strategy returns a single continuous range from the start
   * position to the end position without further segmentation.
   */
  SINGLE_SPLIT_PER_PARTITION(new SingleSplitPerPartitionStrategy());

  private final transient PubSubTopicPartitionSplitStrategy delegate;

  PartitionSplitStrategy(PubSubTopicPartitionSplitStrategy delegate) {
    this.delegate = delegate;
  }

  @Override
  public List<PubSubPartitionSplit> split(SplitRequest request) {
    return delegate.split(request);
  }
}
