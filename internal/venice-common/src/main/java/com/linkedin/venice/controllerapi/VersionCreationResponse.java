package com.linkedin.venice.controllerapi;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import java.util.Collections;
import java.util.Map;


public class VersionCreationResponse extends VersionResponse {
  /* Uses Json Reflective Serializer, get without set may break things */
  private int partitions = 0;
  private int replicas = 0;
  private String kafkaTopic = null;
  private String kafkaBootstrapServers = null;
  // As controller client will ignore the unknown field in json so we could add a field here without break the backward
  // compatibility.
  private boolean enableSSL = false;

  private CompressionStrategy compressionStrategy = CompressionStrategy.NO_OP;

  private String partitionerClass = DefaultVenicePartitioner.class.getName();

  private Map<String, String> partitionerParams = Collections.emptyMap();

  private int amplificationFactor = 1;

  private boolean daVinciPushStatusStoreEnabled = false;

  private String kafkaSourceRegion = null;

  public void setPartitions(int partitions) {
    this.partitions = partitions;
  }

  public void setReplicas(int replicas) {
    this.replicas = replicas;
  }

  public void setKafkaTopic(String kafkaTopic) {
    this.kafkaTopic = kafkaTopic;
  }

  public void setKafkaBootstrapServers(String kafkaBootstrapServers) {
    this.kafkaBootstrapServers = kafkaBootstrapServers;
  }

  public void setKafkaSourceRegion(String kafkaSourceRegion) {
    this.kafkaSourceRegion = kafkaSourceRegion;
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

  public String getKafkaSourceRegion() {
    return kafkaSourceRegion;
  }

  public boolean isEnableSSL() {
    return enableSSL;
  }

  public void setEnableSSL(boolean enableSSL) {
    this.enableSSL = enableSSL;
  }

  public CompressionStrategy getCompressionStrategy() {
    return compressionStrategy;
  }

  public void setCompressionStrategy(CompressionStrategy compressionStrategy) {
    this.compressionStrategy = compressionStrategy;
  }

  public String getPartitionerClass() {
    return partitionerClass;
  }

  public void setPartitionerClass(String partitionerClass) {
    this.partitionerClass = partitionerClass;
  }

  public Map<String, String> getPartitionerParams() {
    return partitionerParams;
  }

  public void setPartitionerParams(Map<String, String> partitionerParams) {
    this.partitionerParams = partitionerParams;
  }

  public int getAmplificationFactor() {
    return amplificationFactor;
  }

  public void setAmplificationFactor(int amplificationFactor) {
    this.amplificationFactor = amplificationFactor;
  }

  public void setDaVinciPushStatusStoreEnabled(boolean daVinciPushStatusStoreEnabled) {
    this.daVinciPushStatusStoreEnabled = daVinciPushStatusStoreEnabled;
  }

  public boolean isDaVinciPushStatusStoreEnabled() {
    return this.daVinciPushStatusStoreEnabled;
  }

  @JsonIgnore
  public String toString() {
    return VersionCreationResponse.class.getSimpleName() + "(partitions: " + partitions + ", replicas: " + replicas
        + ", kafkaTopic: " + kafkaTopic + ", kafkaBootstrapServers: " + kafkaBootstrapServers + ", kafkaSourceRegion: "
        + kafkaSourceRegion + ", enableSSL: " + enableSSL + ", compressionStrategy: " + compressionStrategy.toString()
        + ", partitionerClass: " + partitionerClass + ", partitionerParams: " + partitionerParams
        + ", amplificationFactor: " + amplificationFactor + ", daVinciPushStatusStoreEnabled: "
        + daVinciPushStatusStoreEnabled + ", super: " + super.toString() + ")";
  }
}
