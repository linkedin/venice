package com.linkedin.davinci.storage;

import com.linkedin.davinci.listener.response.AdminResponse;
import com.linkedin.davinci.listener.response.MetadataResponse;
import com.linkedin.venice.utils.ComplementSet;
import java.nio.ByteBuffer;


public interface MetadataRetriever {
  ByteBuffer getStoreVersionCompressionDictionary(String topicName);

  AdminResponse getConsumptionSnapshots(String topicName, ComplementSet<Integer> partitions);

  MetadataResponse getMetadata(String storeName);
}
