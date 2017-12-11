package com.linkedin.venice.storage;

import java.util.Optional;


public interface MetadataRetriever {
  Optional<Long> getOffset(String topicName, int partitionId);
  boolean isStoreVersionChunked(String topicName);
}