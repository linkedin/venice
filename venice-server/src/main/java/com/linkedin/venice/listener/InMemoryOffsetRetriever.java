package com.linkedin.venice.listener;

import java.util.Optional;


public interface InMemoryOffsetRetriever {
  Optional<Long> getOffset(String topicName, int partitionId);
}
