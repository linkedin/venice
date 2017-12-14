package com.linkedin.venice.storage;

import com.linkedin.venice.compression.CompressionStrategy;
import java.util.Optional;


public interface MetadataRetriever {
  Optional<Long> getOffset(String topicName, int partitionId);
  boolean isStoreVersionChunked(String topicName);
  CompressionStrategy getStoreVersionCompressionStrategy(String topicName);
}