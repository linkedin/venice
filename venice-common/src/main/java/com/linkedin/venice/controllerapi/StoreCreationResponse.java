package com.linkedin.venice.controllerapi;

import com.linkedin.venice.ConfigKeys;
import java.util.Map;


/**
 * Created by mwise on 3/17/16.
 */
public class StoreCreationResponse {
  private final String name;
  private final int version;
  private final String owner;
  private final int partitions;
  private final int replicas;
  private final String kafkaTopic;
  private final String kafkaBootstrapServers;

  public StoreCreationResponse(String name, String owner, Map<String, Object> responseMap){
    this.name = name;
    this.owner = owner;
    this.version = (int) responseMap.get(ControllerApiConstants.VERSION);
    this.partitions = (int) responseMap.get(ControllerApiConstants.PARTITIONS);
    this.replicas = (int) responseMap.get(ControllerApiConstants.REPLICAS);
    this.kafkaTopic = (String) responseMap.get(ControllerApiConstants.KAFKA_TOPIC);
    this.kafkaBootstrapServers = (String) responseMap.get(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS);
  }

  public String getName() {
    return name;
  }

  public int getVersion() {
    return version;
  }

  public String getOwner() {
    return owner;
  }

  public int getPartitions() {
    return partitions;
  }

  public int getReplicas() {
    return replicas;
  }

  public String getKafkaTopic() {
    return kafkaTopic;
  }

  public String getKafkaBootstrapServers() {
    return kafkaBootstrapServers;
  }
}
