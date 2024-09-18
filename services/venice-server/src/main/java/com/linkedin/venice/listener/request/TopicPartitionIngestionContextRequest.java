package com.linkedin.venice.listener.request;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.protocols.IngestionContextRequest;


public class TopicPartitionIngestionContextRequest {
  private final String versionTopic;
  private final String topic;
  private final Integer partition;

  private TopicPartitionIngestionContextRequest(String versionTopic, String topic, Integer partition) {
    this.versionTopic = versionTopic;
    this.topic = topic;
    this.partition = partition;
  }

  public static TopicPartitionIngestionContextRequest parseGetHttpRequest(String uri, String[] requestParts) {
    if (requestParts.length == 5) {
      // [0]""/[1]"action"/[2]"version topic"/[3]"topic name"/[4]"partition number"
      String versionTopic = requestParts[2];
      String topic = requestParts[3];
      Integer partition = Integer.valueOf(requestParts[4]);
      return new TopicPartitionIngestionContextRequest(versionTopic, topic, partition);
    } else {
      throw new VeniceException("not a valid request for a TopicPartitionIngestionContext action: " + uri);
    }
  }

  public static TopicPartitionIngestionContextRequest parseGetGrpcRequest(IngestionContextRequest request) {
    return new TopicPartitionIngestionContextRequest(
        request.getVersionTopicName(),
        request.getTopicName(),
        request.getPartition());
  }

  public String getVersionTopic() {
    return versionTopic;
  }

  public String getTopic() {
    return topic;
  }

  public Integer getPartition() {
    return partition;
  }
}
