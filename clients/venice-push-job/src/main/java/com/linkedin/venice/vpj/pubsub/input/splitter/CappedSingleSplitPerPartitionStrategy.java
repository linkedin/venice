package com.linkedin.venice.vpj.pubsub.input.splitter;

import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.vpj.pubsub.input.PubSubPartitionSplit;
import com.linkedin.venice.vpj.pubsub.input.SplitRequest;
import java.util.Collections;
import java.util.List;


public class CappedSingleSplitPerPartitionStrategy implements PubSubTopicPartitionSplitStrategy {
  @Override
  public List<PubSubPartitionSplit> split(SplitRequest request) {
    TopicManager topicManager = request.getTopicManager();
    PubSubTopicPartition topicPartition = request.getPubSubTopicPartition();
    PubSubPosition start = request.getStartPosition();
    long count = Math.min(request.getRecordsPerSplit(), Math.max(0L, request.getNumberOfRecords()));
    PubSubPosition end = count == 0 ? start : topicManager.advancePosition(topicPartition, start, count);
    return Collections.singletonList(
        new PubSubPartitionSplit(topicManager.getTopicRepository(), topicPartition, start, end, count, 0, 0L));
  }
}
