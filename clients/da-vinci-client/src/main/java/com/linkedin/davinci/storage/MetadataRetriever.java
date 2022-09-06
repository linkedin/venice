package com.linkedin.davinci.storage;

import com.linkedin.davinci.listener.response.AdminResponse;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.utils.ComplementSet;
import java.nio.ByteBuffer;


public interface MetadataRetriever {
  boolean isStoreVersionChunked(String topicName);

  CompressionStrategy getStoreVersionCompressionStrategy(String topicName);

  ByteBuffer getStoreVersionCompressionDictionary(String topicName);

  AdminResponse getConsumptionSnapshots(String topicName, ComplementSet<Integer> partitions);
}
