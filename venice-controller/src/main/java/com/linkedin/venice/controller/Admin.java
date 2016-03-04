package com.linkedin.venice.controller;

/**
 * Created by athirupa on 2/1/16.
 */
public interface Admin {
    public void start(String clusterName, VeniceControllerClusterConfig config);

    public void addStore(String clusterName, String storeName, String owner);

    public void addVersion(String clusterName, String storeName, int versionNumber);

    public void addVersion(String clusterName, String storeName, int versionNumber, int numberOfPartition,
        int replicaFactor);

    public void incrementVersion(String clusterName, String storeName, int numberOfPartition, int replicaFactor);

    public void incrementVersion(String clusterName, String storeName);

    public void addKafkaTopic(String clusterName, String kafkaTopic);

    public void addKafkaTopic(String clusterName, String kafkaTopic, int numberOfPartition, int replicaFactor,
        int kafkaReplicaFactor);

    public void stop(String clusterName);
}
