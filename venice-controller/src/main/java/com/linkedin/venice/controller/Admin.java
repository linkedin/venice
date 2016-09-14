package com.linkedin.venice.controller;

import com.linkedin.venice.helix.Replica;
import com.linkedin.venice.job.ExecutionStatus;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.schema.SchemaEntry;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.apache.helix.model.ExternalView;

public interface Admin {
    void start(String clusterName);

    boolean isClusterValid(String clusterName);

    void addStore(String clusterName, String storeName, String owner, String keySchema, String valueSchema);

    Version addVersion(String clusterName, String storeName, int versionNumber, int numberOfPartition,
        int replicaFactor);

    Version incrementVersion(String clusterName, String storeName, int numberOfPartition, int replicaFactor);

    int getCurrentVersion(String clusterName, String storeName);

    Version peekNextVersion(String clusterName, String storeName);

    List<Version> versionsForStore(String clusterName, String storeName);

    List<Store> getAllStores(String clusterName);
    Store getStore(String clusterName, String storeName);

    boolean hasStore(String clusterName, String storeName);

    void reserveVersion(String clusterName, String storeName, int versionNumberToReserve);

    void setCurrentVersion(String clusterName, String storeName, int versionNumber);

    void startOfflinePush(String clusterName, String kafkaTopic, int numberOfPartition, int replicaFactor);

    void deleteOldStoreVersion(String clusterName, String kafkaTopic);

    SchemaEntry getKeySchema(String clusterName, String storeName);

    Collection<SchemaEntry> getValueSchemas(String clusterName, String storeName);

    int getValueSchemaId(String clusterName, String storeName, String valueSchemaStr);

    SchemaEntry getValueSchema(String clusterName, String storeName, int id);

    SchemaEntry addValueSchema(String clusterName, String storeName, String valueSchemaStr);

    SchemaEntry addValueSchema(String clusterName, String storeName, String valueSchemaStr, int schemaId);

    List<String> getStorageNodes(String clusterName);

    void stop(String clusterName);

    /**
     * Query the status of the offline job by given kafka topic.
     * TODO We use kafka topic to tracking the status now but in the further we should use jobId instead of kafka
     * TODO topic. Right now each kafka topic only have one offline job. But in the further one kafka topic could be
     * TODO assigned multiple jobs like data migration job etc.
     * @param clusterName
     * @param kafkaTopic
     * @return the map of job Id to job status.
     */
    ExecutionStatus getOffLineJobStatus(String clusterName, String kafkaTopic);

    /**
     * TODO : Currently bootstrap servers are common per Venice Controller cluster
     * This needs to be configured at per store level or per version level.
     * The Kafka bootstrap servers should also be dynamically sent to the Storage Nodes
     * and only controllers should be aware of them.
     *
     * @return kafka bootstrap servers url, if there are multiple will be comma separated.
     */
    String getKafkaBootstrapServers();

    TopicManager getTopicManager();

    /**
     * Check if this controller itself is the master controller of given cluster or not.
     * @param clusterName
     * @return
     */
    boolean isMasterController(String clusterName);

    /**
    * Calculate how many partitions are needed for the given store and size.
    * @param storeName
    * @param storeSize
    * @return
    */
    int calculateNumberOfPartitions(String clusterName, String storeName, long storeSize);

    int getReplicaFactor(String clusterName, String storeName);

    List<Replica> getBootstrapReplicas(String clusterName, String kafkaTopic);
    List<Replica> getErrorReplicas(String clusterName, String kafkaTopic);
    List<Replica> getReplicas(String clusterName, String kafkaTopic);

    List<Replica> getReplicasOfStorageNode(String clusterName, String instanceId);
    /**
     * Is the given instance able to remove out from given cluster. For example, if there is only one replica alive in this
     * cluster which is hosted on given instance. This instance should not be removed out of cluster, otherwise Venice will
     * lose data.
     *
     * @param helixNodeId nodeId of helix participant. HOST_PORT.
     */
    boolean isInstanceRemovable(String clusterName, String helixNodeId);

    /**
     * Get instance of master controller. If there is no master controller for the given cluster, throw a
     * VeniceException.
     */
    Instance getMasterController(String clusterName);

    /**
    * Pause a store will stop push from going to this store.
    */
    void pauseStore(String clusterName, String storeName);

    /**
    * Resume a store will allow push to go to this store.
    */
    void resumeStore(String clusterName, String storeName);

    void addInstanceToWhitelist(String clusterName, String helixNodeId);

    void removeInstanceFromWhiteList(String clusterName, String helixNodeId);

    Set<String> getWhitelist(String clusterName);

    void close();

    /**
     * Function used by {@link com.linkedin.venice.controller.kafka.consumer.AdminConsumptionTask} to share
     * exception received during consuming admin messages.
     */
    void setLastException(String clusterName, Exception e);

  /**
   * Function used by {@link VeniceParentHelixAdmin} to pick up the latest exception occurred
   * while consuming admin messages.
   * @return
   */
  Exception getLastException(String clusterName);
}
