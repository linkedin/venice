package com.linkedin.davinci.storage;

import com.linkedin.davinci.listener.response.AdminResponse;
import com.linkedin.davinci.listener.response.ReplicaIngestionResponse;
import com.linkedin.venice.utils.ComplementSet;
import java.nio.ByteBuffer;


public interface IngestionMetadataRetriever {
  ByteBuffer getStoreVersionCompressionDictionary(String topicName);

  AdminResponse getConsumptionSnapshots(String topicName, ComplementSet<Integer> partitions);

  ReplicaIngestionResponse getTopicPartitionIngestionContext(String versionTopic, String topicName, int partitionNum);

  ReplicaIngestionResponse getHeartbeatLag(String versionTopicName, int partitionFilter, boolean filterLagReplica);
}
