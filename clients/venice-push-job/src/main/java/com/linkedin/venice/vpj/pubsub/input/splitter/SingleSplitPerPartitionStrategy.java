package com.linkedin.venice.vpj.pubsub.input.splitter;

import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.vpj.pubsub.input.PubSubPartitionSplit;
import com.linkedin.venice.vpj.pubsub.input.SplitRequest;
import java.util.Collections;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Produces exactly one split per {@link PubSubTopicPartition}.
 * This strategy returns a single continuous range from the start
 * position to the end position without further segmentation.
 */
public class SingleSplitPerPartitionStrategy implements PubSubTopicPartitionSplitStrategy {
  private static final Logger LOGGER = LogManager.getLogger(SingleSplitPerPartitionStrategy.class);

  @Override
  public List<PubSubPartitionSplit> split(SplitRequest splitRequest) {
    PubSubTopicPartition pubSubTopicPartition = splitRequest.getPubSubTopicPartition();
    PubSubPosition startPosition = splitRequest.getStartPosition();
    PubSubPosition endPosition = splitRequest.getEndPosition();
    long numberOfRecords = splitRequest.getNumberOfRecords();
    if (numberOfRecords <= 0) {
      return Collections.emptyList();
    }
    LOGGER.debug(
        "Created split-0 for TP: {} record count: {}, start: {}, end: {}",
        pubSubTopicPartition,
        numberOfRecords,
        startPosition,
        endPosition);
    LOGGER.info("Created 1 split for TP: {} with {} records", pubSubTopicPartition, numberOfRecords);
    return Collections.singletonList(
        new PubSubPartitionSplit(
            splitRequest.getTopicManager().getTopicRepository(),
            pubSubTopicPartition,
            startPosition,
            endPosition,
            numberOfRecords,
            0,
            0));
  }
}
