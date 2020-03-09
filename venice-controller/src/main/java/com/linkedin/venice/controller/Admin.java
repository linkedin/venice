package com.linkedin.venice.controller;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controller.kafka.consumer.AdminConsumerService;
import com.linkedin.venice.controller.kafka.consumer.VeniceControllerConsumerFactory;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.helix.Replica;
import com.linkedin.venice.meta.*;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.schema.DerivedSchemaEntry;
import com.linkedin.venice.schema.SchemaEntry;

import com.linkedin.venice.schema.avro.DirectionalSchemaCompatibilityType;
import com.linkedin.venice.status.protocol.PushJobDetails;
import com.linkedin.venice.status.protocol.PushJobStatusRecordKey;
import com.linkedin.venice.status.protocol.PushJobStatusRecordValue;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.writer.VeniceWriterFactory;
import java.io.Closeable;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;


public interface Admin extends AutoCloseable, Closeable {
    // Wrapper to include both overall offline push status and other extra useful info
    class OfflinePushStatusInfo {
        private ExecutionStatus executionStatus;
        private Map<String, String> extraInfo;
        private Optional<String> statusDetails;
        private Map<String, String> extraDetails;

        /** N.B.: Test-only constructor ): */
        public OfflinePushStatusInfo(ExecutionStatus executionStatus) {
            this(executionStatus, new HashMap<>());
        }

        /** N.B.: Test-only constructor ): */
        public OfflinePushStatusInfo(ExecutionStatus executionStatus, Map<String, String> extraInfo) {
            this(executionStatus, extraInfo, Optional.empty(), new HashMap<>());
        }

        /** Used by single datacenter (child) controllers, hence, no extra info nor extra details */
        public OfflinePushStatusInfo(ExecutionStatus executionStatus, Optional<String> statusDetails) {
            this(executionStatus, new HashMap<>(), statusDetails, new HashMap<>());
        }


        /** Used by the parent controller, hence, there is extra info and details about the child */
        public OfflinePushStatusInfo(ExecutionStatus executionStatus, Map<String, String> extraInfo, Optional<String> statusDetails, Map<String, String> extraDetails) {
            this.executionStatus = executionStatus;
            this.extraInfo = extraInfo;
            this.statusDetails = statusDetails;
            this.extraDetails = extraDetails;
        }
        public ExecutionStatus getExecutionStatus() {
            return executionStatus;
        }
        public Map<String, String> getExtraInfo() {
            return extraInfo;
        }
        public Optional<String> getStatusDetails() {
            return statusDetails;
        }
        public Map<String, String> getExtraDetails() {
            return extraDetails;
        }
    }
    void start(String clusterName);

    boolean isClusterValid(String clusterName);

    void addStore(String clusterName, String storeName, String owner, String keySchema, String valueSchema);

    void migrateStore(String srcClusterName, String destClusterName, String storeName);

    void abortMigration(String srcClusterName, String destClusterName, String storeName);

    /**
    * Delete the entire store includeing both metadata and real user's data. Before deleting a store, we should disable
    * the store manually to ensure there is no reading/writing request hitting this tore.
    */
    void deleteStore(String clusterName, String storeName, int largestUsedVersionNumber);

    /**
     * TODO this method should only be in {@link VeniceHelixAdmin}. In the interface only because of store migration.
     * TODO Remove this method from the interface once store migration is refactored.
     */
    void addVersionAndStartIngestion(String clusterName, String storeName, String pushJobId, int versionNumber,
        int numberOfPartitions, Version.PushType pushType);

    /**
     * The implementation of this method must take no action and return the same Version object if the same parameters
     * are provided on a subsequent invocation.  The expected use is multiple distributed components of a single push
     * (with a single jobPushId) that each need to query Venice for the Version (and Kafka topic) to write into.  The
     * first task triggers a new Version, all subsequent tasks identify with the same jobPushId, and should be provided
     * with the same Version object.
     */
    default Version incrementVersionIdempotent(String clusterName, String storeName, String pushJobId, int numberOfPartitions, int replicationFactor) {
        return incrementVersionIdempotent(clusterName, storeName, pushJobId, numberOfPartitions, replicationFactor, Version.PushType.BATCH, false, false);
    }

    Version incrementVersionIdempotent(String clusterName, String storeName, String pushJobId, int numberOfPartitions,
        int replicationFactor, Version.PushType pushType, boolean sendStartOfPush, boolean sorted);

    String getRealTimeTopic(String clusterName, String storeName);

    /**
     * Right now, it will return the latest version recorded in parent controller. There are a couple of edge cases.
     * 1. If a push fails in some colos, the version will be inconsistent among colos
     * 2. If rollback happens, latest version will not be the current version.
     *
     * TODO: figure out how we'd like to cover these edge cases
     */
    Version getIncrementalPushVersion(String clusterName, String storeName);

    int getCurrentVersion(String clusterName, String storeName);

    Map<String, Integer> getCurrentVersionsForMultiColos(String clusterName, String storeName);

    Version peekNextVersion(String clusterName, String storeName);

    /**
     * Delete all of venice versions in given store(including venice resource, kafka topic, offline pushs and all related
     * resources).
     *
     * @throws com.linkedin.venice.exceptions.VeniceException If the given store was not disabled, an exception would be
     *                                                        thrown to reject deletion request.
     */
    List<Version> deleteAllVersionsInStore(String clusterName, String storeName);

    /**
    * Delete the given version from the store. If the given version is the current version, an exception will be thrown.
    */
    void deleteOldVersionInStore(String clusterName, String storeName, int versionNum);

    List<Version> versionsForStore(String clusterName, String storeName);

    List<Store> getAllStores(String clusterName);

    /**
     * Get the statuses of all stores. The store status is decided by the current version. For example, if one partition
     * only have 2 ONLINE replicas in the current version, we say this store is under replicated. Refer to {@link
     * com.linkedin.venice.meta.StoreStatus} for the definition of each status.
     *
     * @return a map which's key is store name and value is store's status.
     */
    Map<String,String> getAllStoreStatuses(String clusterName);

    Store getStore(String clusterName, String storeName);

    boolean hasStore(String clusterName, String storeName);

    SchemaEntry getKeySchema(String clusterName, String storeName);

    Collection<SchemaEntry> getValueSchemas(String clusterName, String storeName);

    Collection<DerivedSchemaEntry> getDerivedSchemas(String clusterName, String storeName);

    int getValueSchemaId(String clusterName, String storeName, String valueSchemaStr);

    Pair<Integer, Integer> getDerivedSchemaId(String clusterName, String storeName, String schemaStr);

    SchemaEntry getValueSchema(String clusterName, String storeName, int id);

    SchemaEntry addValueSchema(String clusterName, String storeName, String valueSchemaStr, DirectionalSchemaCompatibilityType expectedCompatibilityType);

    /**
     * This method skips most of precondition checks and is intended for only internal use.
     * Code from outside should call
     * {@link #addValueSchema(String, String, String, DirectionalSchemaCompatibilityType)} instead.
     *
     * TODO: make it private and remove from the interface list
     */
    SchemaEntry addValueSchema(String clusterName, String storeName, String valueSchemaStr, int schemaId);

    SchemaEntry addSupersetSchema(String clusterName, String storeName, String valueSchemaStr, int valueSchemaId,
        String supersetSchemaStr, int supersetSchemaId);


    DerivedSchemaEntry addDerivedSchema(String clusterName, String storeName, int valueSchemaId, String derivedSchemaStr);

    /**
     * This method skips most of precondition checks and is intended for only internal use.
     */
    DerivedSchemaEntry addDerivedSchema(String clusterName, String storeName, int valueSchemaId, int derivedSchemaId, String derivedSchemaStr);

    void setStoreCurrentVersion(String clusterName, String storeName, int versionNumber);

    void setStoreLargestUsedVersion(String clusterName, String storeName, int versionNumber);

    void setStoreOwner(String clusterName, String storeName, String owner);

    void setStorePartitionCount(String clusterName, String storeName, int partitionCount);

    void setStoreReadability(String clusterName, String storeName, boolean desiredReadability);

    void setStoreWriteability(String clusterName, String storeName, boolean desiredWriteability);

    void setStoreReadWriteability(String clusterName, String storeName, boolean isAccessible);

    void setLeaderFollowerModelEnabled(String clusterName, String storeName, boolean leaderFollowerModelEnabled);

    //TODO: using Optional here is a bit of cumbersome, might want to change it if we find better way to pass those params.
    void updateStore(String clusterName,
                     String storeName,
                     Optional<String> owner,
                     Optional<Boolean> readability,
                     Optional<Boolean> writeability,
                     Optional<Integer> partitionCount,
                     Optional<String> partitionerClass,
                     Optional<Map<String, String>> partitionerParams,
                     Optional<Integer> amplificationFactor,
                     Optional<Long> storageQuotaInByte,
                     Optional<Boolean> hybridStoreDbOverheadBypass,
                     Optional<Long> readQuotaInCU,
                     Optional<Integer> currentVersion,
                     Optional<Integer> largestUsedVersionNumber,
                     Optional<Long> hybridRewindSeconds,
                     Optional<Long> hybridOffsetLagThreshold,
                     Optional<Boolean> accessControlled,
                     Optional<CompressionStrategy> compressionStrategy,
                     Optional<Boolean> clientDecompressionEnabled,
                     Optional<Boolean> chunkingEnabled,
                     Optional<Boolean> singleGetRouterCacheEnabled,
                     Optional<Boolean> batchGetRouterCacheEnabled,
                     Optional<Integer> batchGetLimit,
                     Optional<Integer> numVersionsToPreserve,
                     Optional<Boolean> incrementalPushEnabled,
                     Optional<Boolean> storeMigration,
                     Optional<Boolean> writeComputationEnabled,
                     Optional<Boolean> readComputationEnabled,
                     Optional<Integer> bootstrapToOnlineTimeoutInHours,
                     Optional<Boolean> leaderFollowerModelEnabled,
                     Optional<BackupStrategy> backupStrategy,
                     Optional<Boolean> autoSchemaRegisterPushJobEnabled,
                     Optional<Boolean> autoSupersetSchemaEnabledForReadComputeStore,
                     Optional<Boolean> hybridStoreDiskQuotaEnabled,
                     Optional<Boolean> regularVersionETLEnabled,
                     Optional<Boolean> futureVersionETLEnabled,
                     Optional<String> etledProxyUserAccount);

    default void updateStore(String clusterName, String storeName, UpdateStoreQueryParams params) {
        updateStore(
            clusterName,
            storeName,
            params.getOwner(),
            params.getEnableReads(),
            params.getEnableWrites(),
            params.getPartitionCount(),
            params.getPartitionerClass(),
            params.getPartitionerParams(),
            params.getAmplificationFactor(),
            params.getStorageQuotaInByte(),
            params.getHybridStoreOverheadBypass(),
            params.getReadQuotaInCU(),
            params.getCurrentVersion(),
            params.getLargestUsedVersionNumber(),
            params.getHybridRewindSeconds(),
            params.getHybridOffsetLagThreshold(),
            params.getAccessControlled(),
            params.getCompressionStrategy(),
            params.getClientDecompressionEnabled(),
            params.getChunkingEnabled(),
            params.getSingleGetRouterCacheEnabled(),
            params.getBatchGetRouterCacheEnabled(),
            params.getBatchGetLimit(),
            params.getNumVersionsToPreserve(),
            params.getIncrementalPushEnabled(),
            params.getStoreMigration(),
            params.getWriteComputationEnabled(),
            params.getReadComputationEnabled(),
            params.getBootstrapToOnlineTimeoutInHours(),
            params.getLeaderFollowerModelEnabled(),
            params.getBackupStrategy(),
            params.getAutoSchemaRegisterPushJobEnabled(),
            params.getAutoSupersetSchemaEnabledForReadComputeStore(),
            params.getHybridStoreDiskQuotaEnabled(),
            params.getRegularVersionETLEnabled(),
            params.getFutureVersionETLEnabled(),
            params.getETLedProxyUserAccount());
    }

    double getStorageEngineOverheadRatio(String clusterName);

    List<String> getStorageNodes(String clusterName);

    Map<String, String> getStorageNodesStatus(String clusterName);

    void removeStorageNode(String clusterName, String instanceId);

    /**
     * Stop the helix controller for a single cluster.
     * @param clusterName
     */
    void stop(String clusterName);

    /**
     * Stop the entire controller but not only the helix controller for a single cluster.
     */
    void stopVeniceController();

    /**
     * Query the status of the offline push by given kafka topic.
     * TODO We use kafka topic to tracking the status now but in the further we should use jobId instead of kafka
     * TODO topic. Right now each kafka topic only have one offline job. But in the further one kafka topic could be
     * TODO assigned multiple jobs like data migration job etc.
     * @return the status of current offline push for the passed kafka topic
     */
    OfflinePushStatusInfo getOffLinePushStatus(String clusterName, String kafkaTopic);

    OfflinePushStatusInfo getOffLinePushStatus(String clusterName, String kafkaTopic, Optional<String> incrementalPushVersion);

    Map<String, Long> getOfflinePushProgress(String clusterName, String kafkaTopic);

    /**
     * Return the ssl or non-ssl bootstrap servers based on the given flag.
     * @return kafka bootstrap servers url, if there are multiple will be comma separated.
     */
    String getKafkaBootstrapServers(boolean isSSL);

    /**
     * Return whether ssl is enabled for the given store for push.
     */
    boolean isSSLEnabledForPush(String clusterName, String storeName);

    boolean isSslToKafka();

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

    int getReplicationFactor(String clusterName, String storeName);

    /** number of datacenters, 1 if in single cluster mode.  Could be more if this is a parent controller */
    default int getDatacenterCount(String clusterName){
       return 1;
    }

    List<Replica> getReplicas(String clusterName, String kafkaTopic);

    List<Replica> getReplicasOfStorageNode(String clusterName, String instanceId);

    /**
     * Is the given instance able to remove out from given cluster. For example, if there is only one online replica
     * alive in this cluster which is hosted on the given instance. This instance should not be removed out of cluster,
     * otherwise Venice will lose data. For detail criteria please refer to {@link InstanceStatusDecider}
     *
     * @param helixNodeId nodeId of helix participant. HOST_PORT.
     * @param isFromInstanceView If the value is true, it means we will only check the partitions this instance hold.
     *                           E.g. if all replicas of a partition are error, but this instance does not hold any
     *                           replica in this partition, we will skip this partition in the checking.
     *                           If the value is false, we will check all partitions of resources this instance hold.
     */
    NodeRemovableResult isInstanceRemovable(String clusterName, String helixNodeId, boolean isFromInstanceView);

    NodeRemovableResult isInstanceRemovable(String clusterName, String helixNodeId, int minActiveReplicas, boolean isInstanceView);

    /**
     * Get instance of master controller. If there is no master controller for the given cluster, throw a
     * VeniceException.
     */
    Instance getMasterController(String clusterName);

    void addInstanceToWhitelist(String clusterName, String helixNodeId);

    void removeInstanceFromWhiteList(String clusterName, String helixNodeId);

    Set<String> getWhitelist(String clusterName);

    void killOfflinePush(String clusterName, String kafkaTopic);

    /**
     * Query and return the current status of the given storage node. The "storage node status" is composed by "status" of all
     * replicas in that storage node. "status" is an integer value of Helix state: <ul> <li>DROPPED=1</li> <li>ERROR=2</li>
     * <li>OFFLINE=3</li> <li>BOOTSTRAP=4</li> <li>ONLINE=5</li> </ul> So this method will return a map, the key is the
     * replica name which is composed by resource name and partitionId, and the value is the "status" of this replica.
     */
    StorageNodeStatus getStorageNodesStatus(String clusterName, String instanceId);

    /**
     * Compare the current storage node status and the given storage node status to check is the current one is "Newer"
     * or "Equal" to the given one. Compare will go through each of replica in this storage node, if all their
     * statuses values were larger or equal than the statuses value in the given storage node status, We say current
     * storage node status is "Newer" or "Equal " to the given one.
     */
    boolean isStorageNodeNewerOrEqualTo(String clusterName, String instanceId, StorageNodeStatus oldServerStatus);

    /**
     * Enable or disable the delayed rebalance for the given cluster. By default, the delayed reblance is enabled/disabled
     * depends on the cluster's configuration. Through this method, SRE/DEV could enable or disable the delayed reblance
     * temporarily or set a different delayed rebalance time temporarily.
     *
     * @param  delayedTime how long the helix will not rebalance after a server is disconnected. If the given value
     *                     equal or smaller than 0, we disable the delayed rebalance.
     */

    void setDelayedRebalanceTime(String clusterName, long delayedTime);

    /**
    * Get the current delayed rebalance time value for the given cluster
    * @param clusterName
    */
    long getDelayedRebalanceTime(String clusterName);

    void setAdminConsumerService(String clusterName, AdminConsumerService service);

    /**
     * The admin consumption task tries to deal with failures to process an admin message by retrying.  If there is a
     * message that cannot be processed for some reason, we will need to forcibly skip that message in order to unblock
     * the task from consuming subsequent messages.
     * @param clusterName
     * @param offset
     * @param skipDIV tries to skip only the DIV check for the blocking message.
     */
    void skipAdminMessage(String clusterName, long offset, boolean skipDIV);

    /**
     * Get the id of the last succeed execution in this controller.
     */
    Long getLastSucceedExecutionId(String clusterName);

    /**
    * Get the tracker used to track the execution of the admin command for the given cluster.
    */
    Optional<AdminCommandExecutionTracker> getAdminCommandExecutionTracker(String clusterName);

    /**
     * Get the cluster level config for all routers.
     */
    RoutersClusterConfig getRoutersClusterConfig(String clusterName);

    /**
     * Update the cluster level for all routers.
     */
    void updateRoutersClusterConfig(String clusterName, Optional<Boolean> isThrottlingEnable,
        Optional<Boolean> isQuotaRebalancedEnable, Optional<Boolean> isMaxCapaictyProtectionEnabled,
        Optional<Integer> expectedRouterCount);

    Map<String, String> getAllStorePushStrategyForMigration();

    void setStorePushStrategyForMigration(String voldemortStoreName, String strategy);

    /**
     * Return the clusters which the given store belongs to. And it will only work in the master controller of that
     * cluster. For example, storeA belongs to Cluster1, but the current controller is not the master controller of
     * cluster1, it will not return cluster name for that store.
     */
    List<String> getClusterOfStoreInMasterController(String storeName);

    /**
     * Find the cluster which the given store belongs to. Return the pair of the cluster name and the d2 service
     * associated with that cluster.
     *
     * @throws com.linkedin.venice.exceptions.VeniceException if not cluster is found.
     */
    Pair<String, String> discoverCluster(String storeName);

    /**
     * Find the store versions which have at least one bootstrap replica.
     */
    Map<String, String> findAllBootstrappingVersions(String clusterName);

    VeniceWriterFactory getVeniceWriterFactory();

    VeniceControllerConsumerFactory getVeniceConsumerFactory();

    void close();

    /**
     * This function can be used to perform cluster-wide operations which need to be performed by a single process
     * only in the whole cluster. There could be a race condition during master controller failover,
     * and so long operation should have some way of guarding against that.
     * @return
     */
    boolean isMasterControllerOfControllerCluster();

    boolean isTopicTruncated(String topicName);

    boolean isTopicTruncatedBasedOnRetention(long retention);

    /**
     *
     * @param topicName
     * @return false indicates that the truncate operation has already been done before;
     *         true if it's the first time truncating this topic.
     */
    boolean truncateKafkaTopic(String topicName);

    /**
     * Check whether the specified resource is fully removed or not.
     * @param resourceName
     * @return
     */
    boolean isResourceStillAlive(String resourceName);

    void updateClusterDiscovery(String storeName, String oldCluster, String newCluster);

  /**
   * Send the push job status record to the dedicated kafka topic.
   * @param key the key for the push job status record.
   * @param value the value for the push job status record.
   */
    void sendPushJobStatusMessage(PushJobStatusRecordKey key, PushJobStatusRecordValue value);

    void sendPushJobDetails(PushJobStatusRecordKey key, PushJobDetails value);


    void writeEndOfPush(String clusterName, String storeName, int versionNumber, boolean alsoWriteStartOfPush);

    boolean whetherEnableBatchPushFromAdmin();
}