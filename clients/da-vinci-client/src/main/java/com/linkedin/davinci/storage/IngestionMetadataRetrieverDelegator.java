package com.linkedin.davinci.storage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.linkedin.davinci.kafka.consumer.KafkaStoreIngestionService;
import com.linkedin.davinci.kafka.consumer.ReplicaHeartbeatInfo;
import com.linkedin.davinci.listener.response.AdminResponse;
import com.linkedin.davinci.listener.response.ReplicaIngestionResponse;
import com.linkedin.davinci.stats.ingestion.heartbeat.HeartbeatMonitoringService;
import com.linkedin.venice.helix.VeniceJsonSerializer;
import com.linkedin.venice.utils.ComplementSet;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class IngestionMetadataRetrieverDelegator implements IngestionMetadataRetriever {
  private static final Logger LOGGER = LogManager.getLogger(IngestionMetadataRetrieverDelegator.class);
  private final KafkaStoreIngestionService kafkaStoreIngestionService;
  private final HeartbeatMonitoringService heartbeatMonitoringService;

  private final VeniceJsonSerializer<Map<String, ReplicaHeartbeatInfo>> replicaInfoJsonSerializer =
      new VeniceJsonSerializer<>(new TypeReference<Map<String, ReplicaHeartbeatInfo>>() {
      });

  public IngestionMetadataRetrieverDelegator(
      KafkaStoreIngestionService kafkaStoreIngestionService,
      HeartbeatMonitoringService heartbeatMonitoringService) {
    this.kafkaStoreIngestionService = kafkaStoreIngestionService;
    this.heartbeatMonitoringService = heartbeatMonitoringService;
  }

  @Override
  public ByteBuffer getStoreVersionCompressionDictionary(String topicName) {
    return kafkaStoreIngestionService.getStoreVersionCompressionDictionary(topicName);
  }

  @Override
  public AdminResponse getConsumptionSnapshots(String topicName, ComplementSet<Integer> partitions) {
    return kafkaStoreIngestionService.getConsumptionSnapshots(topicName, partitions);
  }

  @Override
  public ReplicaIngestionResponse getTopicPartitionIngestionContext(
      String versionTopic,
      String topicName,
      int partitionNum) {
    return kafkaStoreIngestionService.getTopicPartitionIngestionContext(versionTopic, topicName, partitionNum);
  }

  @Override
  public ReplicaIngestionResponse getHeartbeatLag(
      String versionTopicName,
      int partitionFilter,
      boolean filterLagReplica) {
    ReplicaIngestionResponse response = new ReplicaIngestionResponse();
    try {
      byte[] topicPartitionInfo = replicaInfoJsonSerializer.serialize(
          heartbeatMonitoringService.getHeartbeatInfo(versionTopicName, partitionFilter, filterLagReplica),
          "");
      response.setPayload(topicPartitionInfo);
    } catch (Exception e) {
      response.setError(true);
      response.setMessage(e.getMessage());
      LOGGER.error("Error on get topic partition ingestion context for all consumers", e);
    }
    return response;
  }
}
