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
 * each containing a fixed target number of records.
 */
public class FixedRecordCountSplitStrategy implements PubSubTopicPartitionSplitStrategy {
  private static final Logger LOGGER = LogManager.getLogger(FixedRecordCountSplitStrategy.class);

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

    long recordsPerSplit = splitRequest.getRecordsPerSplit();
    if (recordsPerSplit <= 0) {
      throw new IllegalArgumentException("recordsPerSplit must be > 0, got " + recordsPerSplit);
    }

    // Compute number of splits (ceil division) without risking overflow in addition.
    long splitsLong = (long) Math.ceil((double) numberOfRecords / recordsPerSplit);
    int splits = (splitsLong > Integer.MAX_VALUE) ? Integer.MAX_VALUE : (int) splitsLong;

    List<PubSubPartitionSplit> out = new ArrayList<>(splits);

    PubSubPosition curStart = startPosition;
    long remaining = numberOfRecords;
    int rangeIndex = 0;
    long startOffset = 0L;

    while (remaining > 0 && rangeIndex < splits) {
      long thisSplitCount = Math.min(recordsPerSplit, remaining);

      // Exclusive end = advance from curStart by thisSplitCount
      PubSubPosition curEnd = topicManager.advancePosition(pubSubTopicPartition, curStart, thisSplitCount);

      out.add(
          new PubSubPartitionSplit(
              topicManager.getTopicRepository(),
              pubSubTopicPartition,
              curStart, // inclusive
              curEnd, // exclusive
              thisSplitCount, // numberOfRecords
              rangeIndex,
              startOffset));
      LOGGER.info(
          "Created split-{} for TP: {} record count: {}, start: {}, end: {}",
          rangeIndex,
          pubSubTopicPartition,
          thisSplitCount,
          curStart,
          curEnd);

      startOffset += thisSplitCount;
      // Next iteration
      curStart = curEnd;
      remaining -= thisSplitCount;
      rangeIndex++;
    }

    return out;
  }
}
