package com.linkedin.davinci.storage;

import com.linkedin.davinci.listener.response.AdminResponse;
import com.linkedin.davinci.listener.response.TopicPartitionIngestionContextResponse;
import com.linkedin.venice.utils.ComplementSet;
import java.nio.ByteBuffer;


public interface IngestionMetadataRetriever {
  ByteBuffer getStoreVersionCompressionDictionary(String topicName);

  AdminResponse getConsumptionSnapshots(String topicName, ComplementSet<Integer> partitions);

  TopicPartitionIngestionContextResponse getTopicPartitionIngestionContext(
      String versionTopic,
      String topicName,
      int partitionNum);

  TopicPartitionIngestionContextResponse getIngestionContext();

  TopicPartitionIngestionContextResponse getHeartbeatLag(
      String topicFilter,
      int partitionFilter,
      boolean filterLagReplica);
}
