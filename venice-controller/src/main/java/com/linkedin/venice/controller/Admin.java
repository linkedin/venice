package com.linkedin.venice.controller;

import com.linkedin.venice.job.ExecutionStatus;
import java.util.Map;


/**
 * Created by athirupa on 2/1/16.
 */
public interface Admin {
    public void start(String clusterName, VeniceControllerClusterConfig config);

    public void addStore(String clusterName, String storeName, String owner);

    public void addVersion(String clusterName, String storeName, int versionNumber);

    public void addVersion(String clusterName, String storeName, int versionNumber, int numberOfPartition,
        int replicaFactor);

    public int incrementVersion(String clusterName, String storeName, int numberOfPartition, int replicaFactor);

    public void setCurrentVersion(String clusterName, String storeName, int versionNumber);

    public void addKafkaTopic(String clusterName, String kafkaTopic, int numberOfPartition, int replicaFactor,
        int kafkaReplicaFactor);

    public void startOfflinePush(String clusterName, String kafkaTopic, int numberOfPartition, int replicaFactor);

    public void stop(String clusterName);

    /**
     * Query the status of the offline job by given kafka topic.
     * TODO We use kafka topic to tracking the status now but in the further we should use jobId instead of kafka
     * TODO topic. Right now each kafka topic only have one offline job. But in the further one kafka topic could be
     * TODO assigned multiple jobs like data migration job etc.
     * @param clusterName
     * @param kafkaTopic
     * @return the map of job Id to job status.
     */
    public ExecutionStatus getOffLineJobStatus(String clusterName, String kafkaTopic);
}
