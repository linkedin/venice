package com.linkedin.venice.controller;

import com.linkedin.venice.job.ExecutionStatus;
import com.linkedin.venice.meta.Version;


/**
 * Created by athirupa on 2/1/16.
 */
public interface Admin {
    public void start(String clusterName);

    public void addStore(String clusterName, String storeName, String owner);

    public Version addVersion(String clusterName, String storeName, int versionNumber);

    public Version addVersion(String clusterName, String storeName, int versionNumber, int numberOfPartition,
        int replicaFactor);

    public Version incrementVersion(String clusterName, String storeName, int numberOfPartition, int replicaFactor);

    public Version peekNextVersion(String clusterName, String storeName);

    public void reserveVersion(String clusterName, String storeName, int versionNumberToReserve);

    public void setCurrentVersion(String clusterName, String storeName, int versionNumber);

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

    /**
     * TODO : Currently bootstrap servers are common per Venice Controller cluster
     * This needs to be configured at per store level or per version level.
     * The Kafka bootstrap servers should also be dynamically sent to the Storage Nodes
     * and only controllers should be aware of them.
     *
     * @return kafka bootstrap servers url, if there are multiple will be comma separated.
     */
    public String getKafkaBootstrapServers();

    /**
     * Check if this controller itself is the master controller of given cluster or not.
     * @param clusterName
     * @return
     */
    public boolean isMasterController(String clusterName);

    public void close();
}
