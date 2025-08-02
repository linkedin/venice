package com.linkedin.venice.vpj.pubsub.input.splitter;

import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.manager.TopicManager;
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
    TopicManager topicManager = splitRequest.getTopicManager();
    PubSubPosition startPosition = topicManager.getStartPositionsForPartitionWithRetries(pubSubTopicPartition);
    PubSubPosition endPosition = topicManager.getEndPositionsForPartitionWithRetries(pubSubTopicPartition);
    long numberOfRecords = topicManager.diffPosition(pubSubTopicPartition, endPosition, startPosition);
    if (numberOfRecords <= 0) {
      return Collections.emptyList();
    }
    LOGGER.info(
        "Created split-0 for TP: {} record count: {}, start: {}, end: {}",
        pubSubTopicPartition,
        numberOfRecords,
        startPosition,
        endPosition);
    return Collections.singletonList(
        new PubSubPartitionSplit(
            topicManager.getTopicRepository(),
            pubSubTopicPartition,
            startPosition,
            endPosition,
            numberOfRecords,
            0,
            0));
  }
}
