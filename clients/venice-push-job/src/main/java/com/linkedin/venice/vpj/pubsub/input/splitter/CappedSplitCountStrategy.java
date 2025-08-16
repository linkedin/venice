package com.linkedin.venice.vpj.pubsub.input.splitter;

import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.vpj.pubsub.input.PubSubPartitionSplit;
import com.linkedin.venice.vpj.pubsub.input.SplitRequest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Splits a {@link PubSubTopicPartition} into multiple splits,
 * with the total number of splits capped at a configured maximum.
 * The size of each split is calculated by dividing the total record
 * count by this maximum.
 */
public class CappedSplitCountStrategy implements PubSubTopicPartitionSplitStrategy {
  private static final Logger LOGGER = LogManager.getLogger(CappedSplitCountStrategy.class);

  @Override
  public List<PubSubPartitionSplit> split(SplitRequest splitRequest) {
    PubSubTopicPartition pubSubTopicPartition = splitRequest.getPubSubTopicPartition();
    TopicManager topicManager = splitRequest.getTopicManager();
    int maxSplits = Math.max(1, splitRequest.getMaxSplits()); // ensure >= 1

    PubSubPosition startPosition = topicManager.getStartPositionsForPartitionWithRetries(pubSubTopicPartition);
    PubSubPosition endPosition = topicManager.getEndPositionsForPartitionWithRetries(pubSubTopicPartition);

    long numberOfRecords = topicManager.diffPosition(pubSubTopicPartition, endPosition, startPosition);
    if (numberOfRecords <= 0) {
      return Collections.emptyList();
    }

    // You cannot have more splits than records.
    int splits = (int) Math.min(numberOfRecords, maxSplits);

    long base = numberOfRecords / splits; // >= 1
    long remainder = numberOfRecords % splits; // first 'remainder' splits get +1

    List<PubSubPartitionSplit> out = new ArrayList<>(splits);
    PubSubPosition curStart = startPosition;
    long startOffset = 0L;

    for (int i = 0; i < splits; i++) {
      long thisSplitCount = base + (i < remainder ? 1 : 0);
      // Advance from curStart by thisSplitCount to get the exclusive end.
      PubSubPosition curEnd = topicManager.advancePosition(pubSubTopicPartition, curStart, thisSplitCount);

      out.add(
          new PubSubPartitionSplit(
              topicManager.getTopicRepository(),
              pubSubTopicPartition,
              curStart, // inclusive
              curEnd, // exclusive
              thisSplitCount,
              i,
              startOffset));
      LOGGER.info(
          "Created split-{} for TP: {} record count: {}, start: {}, end: {}",
          i,
          pubSubTopicPartition,
          thisSplitCount,
          curStart,
          curEnd);
      curStart = curEnd;
      startOffset += thisSplitCount;
    }

    return out;
  }
}
