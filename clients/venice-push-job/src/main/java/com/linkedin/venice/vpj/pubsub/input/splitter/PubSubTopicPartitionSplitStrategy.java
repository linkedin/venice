package com.linkedin.venice.vpj.pubsub.input.splitter;

import com.linkedin.venice.vpj.pubsub.input.PubSubPartitionSplit;
import com.linkedin.venice.vpj.pubsub.input.SplitRequest;
import java.util.List;


public interface PubSubTopicPartitionSplitStrategy {
  List<PubSubPartitionSplit> split(SplitRequest splitRequest);
}
