package com.linkedin.venice.controller;

import com.linkedin.venice.acl.AclException;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.controller.kafka.consumer.AdminConsumerService;
import com.linkedin.venice.controllerapi.NodeReplicasReadinessState;
import com.linkedin.venice.controllerapi.RepushInfo;
import com.linkedin.venice.controllerapi.StoreComparisonInfo;
import com.linkedin.venice.controllerapi.UpdateClusterConfigQueryParams;
import com.linkedin.venice.controllerapi.UpdateStoragePersonaQueryParams;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.helix.HelixReadOnlyStoreConfigRepository;
import com.linkedin.venice.helix.HelixReadOnlyZKSharedSchemaRepository;
import com.linkedin.venice.helix.HelixReadOnlyZKSharedSystemStoreRepository;
import com.linkedin.venice.helix.Replica;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.RegionPushDetails;
import com.linkedin.venice.meta.RoutersClusterConfig;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreDataAudit;
import com.linkedin.venice.meta.StoreGraveyard;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.UncompletedPartition;
import com.linkedin.venice.meta.VeniceUserStoreType;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.persona.StoragePersona;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterFactory;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushstatushelper.PushStatusStoreReader;
import com.linkedin.venice.pushstatushelper.PushStatusStoreWriter;
import com.linkedin.venice.schema.GeneratedSchemaID;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.avro.DirectionalSchemaCompatibilityType;
import com.linkedin.venice.schema.rmd.RmdSchemaEntry;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import com.linkedin.venice.status.protocol.BatchJobHeartbeatKey;
import com.linkedin.venice.status.protocol.BatchJobHeartbeatValue;
import com.linkedin.venice.status.protocol.PushJobDetails;
import com.linkedin.venice.status.protocol.PushJobStatusRecordKey;
import com.linkedin.venice.system.store.MetaStoreReader;
import com.linkedin.venice.system.store.MetaStoreWriter;
import com.linkedin.venice.systemstore.schemas.StoreMetaKey;
import com.linkedin.venice.systemstore.schemas.StoreMetaValue;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriterFactory;
import java.io.Closeable;
import java.io.IOException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.avro.Schema;


public interface Admin extends AutoCloseable, Closeable {
  // Wrapper to include both overall offline push status and other extra useful info
  class OfflinePushStatusInfo {
    private ExecutionStatus executionStatus;
    private Long statusUpdateTimestamp;
    private Map<String, String> extraInfo;
    private Map<String, Long> extraInfoUpdateTimestamp;
    private String statusDetails;
    private Map<String, String> extraDetails;
    private List<UncompletedPartition> uncompletedPartitions;

    /** N.B.: Test-only constructor ): */
    public OfflinePushStatusInfo(ExecutionStatus executionStatus) {
      this(executionStatus, Collections.emptyMap());
    }

    /** N.B.: Test-only constructor ): */
    public OfflinePushStatusInfo(ExecutionStatus executionStatus, Map<String, String> extraInfo) {
      this(executionStatus, null, extraInfo, null, Collections.emptyMap(), Collections.emptyMap());
    }

    /** Used by single datacenter (child) controllers, hence, no extra info nor extra details */
    public OfflinePushStatusInfo(ExecutionStatus executionStatus, Long statusUpdateTimestamp, String statusDetails) {
      this(
          executionStatus,
          statusUpdateTimestamp,
          Collections.emptyMap(),
          statusDetails,
          Collections.emptyMap(),
          Collections.emptyMap());
    }

    /** Used by the parent controller, hence, there is extra info and details about the child */
    public OfflinePushStatusInfo(
        ExecutionStatus executionStatus,
        Long statusUpdateTimestamp,
        Map<String, String> extraInfo,
        String statusDetails,
        Map<String, String> extraDetails,
        Map<String, Long> extraInfoUpdateTimestamp) {
      this.executionStatus = executionStatus;
      this.statusUpdateTimestamp = statusUpdateTimestamp;
      this.extraInfo = extraInfo;
      this.statusDetails = statusDetails;
      this.extraDetails = extraDetails;
      this.extraInfoUpdateTimestamp = extraInfoUpdateTimestamp;
    }

    public ExecutionStatus getExecutionStatus() {
      return executionStatus;
    }

    public Map<String, String> getExtraInfo() {
      return extraInfo;
    }

    public String getStatusDetails() {
      return statusDetails;
    }

    public Map<String, String> getExtraDetails() {
      return extraDetails;
    }

    public List<UncompletedPartition> getUncompletedPartitions() {
      return uncompletedPartitions;
    }

    public void setUncompletedPartitions(List<UncompletedPartition> uncompletedPartitions) {
      this.uncompletedPartitions = uncompletedPartitions;
    }

    public Long getStatusUpdateTimestamp() {
      return this.statusUpdateTimestamp;
    }

    public Map<String, Long> getExtraInfoUpdateTimestamp() {
      return this.extraInfoUpdateTimestamp;
    }
  }

  void initStorageCluster(String clusterName);

  boolean isClusterValid(String clusterName);

  default void createStore(String clusterName, String storeName, String owner, String keySchema, String valueSchema) {
    createStore(clusterName, storeName, owner, keySchema, valueSchema, false, Optional.empty());
  }

  default void createStore(
      String clusterName,
      String storeName,
      String owner,
      String keySchema,
      String valueSchema,
      boolean isSystemStore) {
    createStore(clusterName, storeName, owner, keySchema, valueSchema, isSystemStore, Optional.empty());
  }

  void createStore(
      String clusterName,
      String storeName,
      String owner,
      String keySchema,
      String valueSchema,
      boolean isSystemStore,
      Optional<String> accessPermissions);

  boolean isStoreMigrationAllowed(String srcClusterName);

  void migrateStore(String srcClusterName, String destClusterName, String storeName);

  void completeMigration(String srcClusterName, String destClusterName, String storeName);

  void abortMigration(String srcClusterName, String destClusterName, String storeName);

  /**
  * Delete the entire store including both metadata and real user's data. Before deleting a store, we should disable
  * the store manually to ensure there is no reading/writing request hitting this tore.
  */
  void deleteStore(String clusterName, String storeName, int largestUsedVersionNumber, boolean waitOnRTTopicDeletion);

  /**
   * This method behaves differently in {@link VeniceHelixAdmin} and {@link VeniceParentHelixAdmin}.
   */
  void addVersionAndStartIngestion(
      String clusterName,
      String storeName,
      String pushJobId,
      int versionNumber,
      int numberOfPartitions,
      Version.PushType pushType,
      String remoteKafkaBootstrapServers,
      long rewindTimeInSecondsOverride,
      int replicationMetadataVersionId,
      boolean versionSwapDeferred,
      int repushSourceVersion);

  default boolean hasWritePermissionToBatchJobHeartbeatStore(
      X509Certificate requesterCert,
      String batchJobHeartbeatStoreName) throws AclException {
    return false;
  }

  /**
   * The implementation of this method must take no action and return the same Version object if the same parameters
   * are provided on a subsequent invocation.  The expected use is multiple distributed components of a single push
   * (with a single jobPushId) that each need to query Venice for the Version (and Kafka topic) to write into.  The
   * first task triggers a new Version, all subsequent tasks identify with the same jobPushId, and should be provided
   * with the same Version object.
   */
  default Version incrementVersionIdempotent(
      String clusterName,
      String storeName,
      String pushJobId,
      int numberOfPartitions,
      int replicationFactor) {
    return incrementVersionIdempotent(
        clusterName,
        storeName,
        pushJobId,
        numberOfPartitions,
        replicationFactor,
        Version.PushType.BATCH,
        true,
        false,
        null,
        Optional.empty(),
        Optional.empty(),
        -1,
        Optional.empty(),
        false,
        null,
        -1);
  }

  default Version incrementVersionIdempotent(
      String clusterName,
      String storeName,
      String pushJobId,
      int numberOfPartitions,
      int replicationFactor,
      Version.PushType pushType,
      boolean sendStartOfPush,
      boolean sorted,
      String compressionDictionary,
      Optional<String> sourceGridFabric,
      Optional<X509Certificate> requesterCert,
      long rewindTimeInSecondsOverride,
      Optional<String> emergencySourceRegion,
      boolean versionSwapDeferred,
      int repushSourceVersion) {
    return incrementVersionIdempotent(
        clusterName,
        storeName,
        pushJobId,
        numberOfPartitions,
        replicationFactor,
        pushType,
        sendStartOfPush,
        sorted,
        compressionDictionary,
        sourceGridFabric,
        requesterCert,
        rewindTimeInSecondsOverride,
        emergencySourceRegion,
        versionSwapDeferred,
        null,
        repushSourceVersion);
  }

  Version incrementVersionIdempotent(
      String clusterName,
      String storeName,
      String pushJobId,
      int numberOfPartitions,
      int replicationFactor,
      Version.PushType pushType,
      boolean sendStartOfPush,
      boolean sorted,
      String compressionDictionary,
      Optional<String> sourceGridFabric,
      Optional<X509Certificate> requesterCert,
      long rewindTimeInSecondsOverride,
      Optional<String> emergencySourceRegion,
      boolean versionSwapDeferred,
      String targetedRegions,
      int repushSourceVersion);

  String getRealTimeTopic(String clusterName, Store store);

  default String getRealTimeTopic(String clusterName, String storeName) {
    Store store = getStore(clusterName, storeName);
    if (store == null) {
      throw new VeniceNoStoreException(storeName, clusterName);
    }
    return getRealTimeTopic(clusterName, store);
  }

  String getSeparateRealTimeTopic(String clusterName, String storeName);

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

  Map<String, String> getFutureVersionsForMultiColos(String clusterName, String storeName);

  Map<String, String> getBackupVersionsForMultiColos(String clusterName, String storeName);

  int getBackupVersion(String clusterName, String storeName);

  int getFutureVersion(String clusterName, String storeName);

  RepushInfo getRepushInfo(String clusterNae, String storeName, Optional<String> fabricName);

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
   * @return a map whose key is store name and value is store's status.
   */
  Map<String, String> getAllStoreStatuses(String clusterName);

  Store getStore(String clusterName, String storeName);

  boolean hasStore(String clusterName, String storeName);

  SchemaEntry getKeySchema(String clusterName, String storeName);

  Collection<SchemaEntry> getValueSchemas(String clusterName, String storeName);

  Collection<DerivedSchemaEntry> getDerivedSchemas(String clusterName, String storeName);

  int getValueSchemaId(String clusterName, String storeName, String valueSchemaStr);

  GeneratedSchemaID getDerivedSchemaId(String clusterName, String storeName, String schemaStr);

  SchemaEntry getValueSchema(String clusterName, String storeName, int id);

  SchemaEntry addValueSchema(
      String clusterName,
      String storeName,
      String valueSchemaStr,
      DirectionalSchemaCompatibilityType expectedCompatibilityType);

  SchemaEntry addValueSchema(
      String clusterName,
      String storeName,
      String valueSchemaStr,
      int schemaId,
      DirectionalSchemaCompatibilityType expectedCompatibilityType);

  /**
   * This method skips most precondition checks and is intended for only internal use.
   * Code from outside should call
   * {@link #addValueSchema(String, String, String, DirectionalSchemaCompatibilityType)} instead.
   *
   * @see #addValueSchema(String, String, String, int, DirectionalSchemaCompatibilityType)
   */
  default SchemaEntry addValueSchema(String clusterName, String storeName, String valueSchemaStr, int schemaId) {
    return addValueSchema(
        clusterName,
        storeName,
        valueSchemaStr,
        schemaId,
        SchemaEntry.DEFAULT_SCHEMA_CREATION_COMPATIBILITY_TYPE);
  }

  SchemaEntry addSupersetSchema(
      String clusterName,
      String storeName,
      String valueSchemaStr,
      int valueSchemaId,
      String supersetSchemaStr,
      int supersetSchemaId);

  DerivedSchemaEntry addDerivedSchema(String clusterName, String storeName, int valueSchemaId, String derivedSchemaStr);

  Set<Integer> getInUseValueSchemaIds(String clusterName, String storeName);

  /**
   * Deletes a store's values schema with ids `except` the ids passed in the argument inuseValueSchemaIds
   */
  void deleteValueSchemas(String clusterName, String storeName, Set<Integer> inuseValueSchemaIds);

  StoreMetaValue getMetaStoreValue(StoreMetaKey storeMetaKey, String storeName);

  /**
   * This method skips most precondition checks and is intended for only internal use.
   */
  DerivedSchemaEntry addDerivedSchema(
      String clusterName,
      String storeName,
      int valueSchemaId,
      int derivedSchemaId,
      String derivedSchemaStr);

  Collection<RmdSchemaEntry> getReplicationMetadataSchemas(String clusterName, String storeName);

  Optional<Schema> getReplicationMetadataSchema(
      String clusterName,
      String storeName,
      int valueSchemaID,
      int rmdVersionID);

  RmdSchemaEntry addReplicationMetadataSchema(
      String clusterName,
      String storeName,
      int valueSchemaId,
      int replicationMetadataVersionId,
      String replicationMetadataSchemaStr);

  void validateAndMaybeRetrySystemStoreAutoCreation(
      String clusterName,
      String storeName,
      VeniceSystemStoreType systemStoreType);

  /**
   * Remove an existing derived schema
   * @return the derived schema that is deleted or null if the schema doesn't exist
   */
  DerivedSchemaEntry removeDerivedSchema(String clusterName, String storeName, int valueSchemaId, int derivedSchemaId);

  void setStoreCurrentVersion(String clusterName, String storeName, int versionNumber);

  void rollForwardToFutureVersion(String clusterName, String storeName, String regionFilter);

  void rollbackToBackupVersion(String clusterName, String storeName, String regionFilter);

  void setStoreLargestUsedVersion(String clusterName, String storeName, int versionNumber);

  void setStoreOwner(String clusterName, String storeName, String owner);

  void setStorePartitionCount(String clusterName, String storeName, int partitionCount);

  void setStoreReadability(String clusterName, String storeName, boolean desiredReadability);

  void setStoreWriteability(String clusterName, String storeName, boolean desiredWriteability);

  void setStoreReadWriteability(String clusterName, String storeName, boolean isAccessible);

  void updateStore(String clusterName, String storeName, UpdateStoreQueryParams params);

  void updateClusterConfig(String clusterName, UpdateClusterConfigQueryParams params);

  double getStorageEngineOverheadRatio(String clusterName);

  List<String> getStorageNodes(String clusterName);

  Map<String, String> getStorageNodesStatus(String clusterName, boolean enableReplica);

  void removeStorageNode(String clusterName, String instanceId);

  /**
   * Stop the helix controller for a single cluster.
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

  OfflinePushStatusInfo getOffLinePushStatus(
      String clusterName,
      String kafkaTopic,
      Optional<String> incrementalPushVersion,
      String region,
      String targetedRegions);

  /**
   * Return the ssl or non-ssl bootstrap servers based on the given flag.
   * @return kafka bootstrap servers url, if there are multiple will be comma separated.
   */
  String getKafkaBootstrapServers(boolean isSSL);

  /**
   * Return the region name of this Admin
   * @return the region name of this controller
   */
  String getRegionName();

  String getNativeReplicationKafkaBootstrapServerAddress(String sourceFabric);

  String getNativeReplicationSourceFabric(
      String clusterName,
      Store store,
      Optional<String> sourceGridFabric,
      Optional<String> emergencySourceRegion,
      String targetedRegions);

  /**
   * Return whether ssl is enabled for the given store for push.
   */
  boolean isSSLEnabledForPush(String clusterName, String storeName);

  boolean isSslToKafka();

  TopicManager getTopicManager();

  TopicManager getTopicManager(String pubSubServerAddress);

  InstanceRemovableStatuses getAggregatedHealthStatus(
      String cluster,
      List<String> instances,
      List<String> toBeStoppedInstances,
      boolean isSSLEnabled);

  boolean isRTTopicDeletionPermittedByAllControllers(String clusterName, String storeName);

  /**
   * Check if this controller itself is the leader controller for a given cluster or not. Note that the controller can be
   * either a parent controller or a child controller since a cluster must have a leader child controller and a leader
   * parent controller. The point is not to be confused the concept of leader-standby with parent-child controller
   * architecture.
   */
  boolean isLeaderControllerFor(String clusterName);

  /**
  * Calculate how many partitions are needed for the given store.
  */
  int calculateNumberOfPartitions(String clusterName, String storeName);

  int getReplicationFactor(String clusterName, String storeName);

  /** number of datacenters, 1 if in single cluster mode.  Could be more if this is a parent controller */
  default int getDatacenterCount(String clusterName) {
    return 1;
  }

  List<Replica> getReplicas(String clusterName, String kafkaTopic);

  List<Replica> getReplicasOfStorageNode(String clusterName, String instanceId);

  /**
   * Assuming all hosts identified by lockedNodes and their corresponding resources are unusable, is the given instance
   * able to be removed out from the given cluster.
   * For example, if there is only one online replica alive in this cluster which is hosted on the given instance.
   * This instance should not be removed out of cluster, otherwise Venice will lose data.
   * For detail criteria please refer to {@link InstanceStatusDecider}
   *
   * @param clusterName The cluster were the hosts belong.
   * @param helixNodeId nodeId of helix participant. HOST_PORT.
   * @param lockedNodes A list of helix nodeIds whose resources are assumed to be unusable (stopped).
   */
  NodeRemovableResult isInstanceRemovable(String clusterName, String helixNodeId, List<String> lockedNodes);

  /**
   * Get instance of leader controller. If there is no leader controller for the given cluster, throw a
   * VeniceException.
   */
  Instance getLeaderController(String clusterName);

  void addInstanceToAllowlist(String clusterName, String helixNodeId);

  void removeInstanceFromAllowList(String clusterName, String helixNodeId);

  Set<String> getAllowlist(String clusterName);

  /**
   * Kill an offline push if it ran into errors or the corresponding version is being retired.
   * @param clusterName
   * @param kafkaTopic
   * @param isForcedKill should be set to true when killing the push job for retiring the corresponding version.
   */
  void killOfflinePush(String clusterName, String kafkaTopic, boolean isForcedKill);

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
  void updateRoutersClusterConfig(
      String clusterName,
      Optional<Boolean> isThrottlingEnable,
      Optional<Boolean> isQuotaRebalancedEnable,
      Optional<Boolean> isMaxCapaictyProtectionEnabled,
      Optional<Integer> expectedRouterCount);

  Map<String, String> getAllStorePushStrategyForMigration();

  void setStorePushStrategyForMigration(String voldemortStoreName, String strategy);

  /**
   * Find the cluster which the given store belongs to. Return the pair of the cluster name and the d2 service
   * associated with that cluster.
   *
   * @throws com.linkedin.venice.exceptions.VeniceException if not cluster is found.
   */
  Pair<String, String> discoverCluster(String storeName);

  /**
   * Find the server d2 service associated with a given cluster name.
   */
  String getServerD2Service(String clusterName);

  /**
   * Find the store versions which have at least one bootstrap replica.
   */
  Map<String, String> findAllBootstrappingVersions(String clusterName);

  VeniceWriterFactory getVeniceWriterFactory();

  PubSubConsumerAdapterFactory getPubSubConsumerAdapterFactory();

  VeniceProperties getPubSubSSLProperties(String pubSubBrokerAddress);

  void close();

  /**
   * This function can be used to perform cluster-wide operations which need to be performed by a single process
   * only in the whole cluster. There could be a race condition during leader controller failover,
   * and so long operation should have some way of guarding against that.
   */
  boolean isLeaderControllerOfControllerCluster();

  boolean isTopicTruncated(String topicName);

  boolean isTopicTruncatedBasedOnRetention(long retention);

  boolean isTopicTruncatedBasedOnRetention(String topicName, long retention);

  int getMinNumberOfUnusedKafkaTopicsToPreserve();

  /**
   * @return false indicates that the truncate operation has already been done before;
   *         true if it's the first time truncating this topic.
   */
  boolean truncateKafkaTopic(String topicName);

  /**
   * Truncate a Kafka topic by setting its retention time to the input value.
   * @param topicName the name of the topic to truncate.
   * @param retentionTimeInMs the retention time in milliseconds to set for the topic.
   * @return true if truncating this topic successfully.
   *        false otherwise.
   */
  boolean truncateKafkaTopic(String topicName, long retentionTimeInMs);

  /**
   * Check whether the specified resource is fully removed or not.
   */
  boolean isResourceStillAlive(String resourceName);

  /**
   * Update the cluster discovery of a given store by writing to the StoreConfig ZNode.
   * @param storeName of the store.
   * @param oldCluster for the store.
   * @param newCluster for the store.
   * @param initiatingCluster that is making the update. This is needed because in the case of store migration
   *                          sometimes the update is not made by the leader of the current cluster but instead the
   *                          leader of the source cluster.
   */
  void updateClusterDiscovery(String storeName, String oldCluster, String newCluster, String initiatingCluster);

  void sendPushJobDetails(PushJobStatusRecordKey key, PushJobDetails value);

  PushJobDetails getPushJobDetails(PushJobStatusRecordKey key);

  BatchJobHeartbeatValue getBatchJobHeartbeatValue(BatchJobHeartbeatKey batchJobHeartbeatKey);

  void writeEndOfPush(String clusterName, String storeName, int versionNumber, boolean alsoWriteStartOfPush);

  boolean whetherEnableBatchPushFromAdmin(String storeName);

  /**
   * Provision a new set of ACL for a venice store and its associated kafka topic.
   */
  void updateAclForStore(String clusterName, String storeName, String accessPermisions);

  /**
   * Fetch the current set of ACL provisioned for a venice store and its associated kafka topic.
   * @return The string representation of the accessPermissions. It will return empty string in case store is not present.
   */
  String getAclForStore(String clusterName, String storeName);

  /**
   * Delete the current set of ACL provisioned for a venice store and its associated kafka topic.
   */
  void deleteAclForStore(String clusterName, String storeName);

  /**
   * Check whether the controller works as a parent controller
   * @return true if it works as a parent controller. Otherwise, return false.
   */
  boolean isParent();

  /**
   * Return the state of the region of the parent controller.
   * @return {@link ParentControllerRegionState#ACTIVE} which means that the parent controller in the region is serving requests.
   * Otherwise, return {@link ParentControllerRegionState#PASSIVE}
   */
  ParentControllerRegionState getParentControllerRegionState();

  /**
   * Get child datacenter to child controller url mapping.
   * @return A map of child datacenter -> child controller url
   */
  Map<String, String> getChildDataCenterControllerUrlMap(String clusterName);

  /**
   * Get child datacenter to child controller d2 zk host mapping
   * @return A map of child datacenter -> child controller d2 zk host
   */
  Map<String, String> getChildDataCenterControllerD2Map(String clusterName);

  /**
   * Get child datacenter controller d2 service name
   * @return d2 service name
   */
  String getChildControllerD2ServiceName(String clusterName);

  /**
   * Return a shared store config repository.
   */
  HelixReadOnlyStoreConfigRepository getStoreConfigRepo();

  /**
   * Return a shared read only store repository for zk shared stores.
   */
  HelixReadOnlyZKSharedSystemStoreRepository getReadOnlyZKSharedSystemStoreRepository();

  /**
   * Return a shared read only schema repository for zk shared stores.
   */
  HelixReadOnlyZKSharedSchemaRepository getReadOnlyZKSharedSchemaRepository();

  /**
   * Return a {@link MetaStoreWriter}, which can be shared across different Venice clusters.
   */
  MetaStoreWriter getMetaStoreWriter();

  MetaStoreReader getMetaStoreReader();

  /** Get a list of clusters this controller is a leader of.
   * @return a list of clusters this controller is a leader of.
   */
  List<String> getClustersLeaderOf();

  /**
   * Enable/disable active active replications for certain stores (batch only, hybrid only, incremental push, hybrid or incremental push,
   * all) in a cluster. If storeName is not empty, only the specified store might be updated.
   */
  void configureActiveActiveReplication(
      String cluster,
      VeniceUserStoreType storeType,
      Optional<String> storeName,
      boolean enableActiveActiveReplicationForCluster,
      Optional<String> regionsFilter);

  /**
   * Check whether there are any resource left for the store creation in cluster: {@param clusterName}
   * If there is any, this function should throw Exception.
   */
  void checkResourceCleanupBeforeStoreCreation(String clusterName, String storeName);

  /**
   * Return the emergency source region configuration.
   */
  Optional<String> getEmergencySourceRegion(String clusterName);

  /**
   * Return the source Kafka boostrap server url for aggregate real-time topic updates
   */
  Optional<String> getAggregateRealTimeTopicSource(String clusterName);

  /**
   * Returns true if A/A replication is enabled in all child controller and parent controller. This is implemented only in parent controller.
   * Otherwise, return false.
   */
  boolean isActiveActiveReplicationEnabledInAllRegion(
      String clusterName,
      String storeName,
      boolean checkCurrentVersion);

  /**
   * Returns default backup version retention time.
   */
  long getBackupVersionDefaultRetentionMs();

  /**
   * @return The default value of {@link com.linkedin.venice.writer.VeniceWriter#maxRecordSizeBytes} which is
   * provided to the VPJ and Consumer as a controller config to dynamically control the setting per cluster.
   */
  int getDefaultMaxRecordSizeBytes();

  void wipeCluster(String clusterName, String fabric, Optional<String> storeName, Optional<Integer> versionNum);

  StoreInfo copyOverStoreSchemasAndConfigs(String clusterName, String srcFabric, String destFabric, String storeName);

  /**
   * Compare store metadata and version states between two fabrics.
   */
  StoreComparisonInfo compareStore(String clusterName, String storeName, String fabricA, String fabricB)
      throws IOException;

  /**
   *
   * helixNodeId nodeId of helix participant. HOST_PORT.
   * Returns ture, if all current version replicas of the input node are ready to serve.
   *         false and all unready replicas otherwise.
   */
  Pair<NodeReplicasReadinessState, List<Replica>> nodeReplicaReadiness(String cluster, String helixNodeId);

  /**
   * Initiate data recovery for a store version given a source fabric.
   * @param clusterName of the store.
   * @param storeName of the store.
   * @param version of the store.
   * @param sourceFabric to be used as the source for data recovery.
   * @param copyAllVersionConfigs a boolean to indicate whether all version configs should be copied from the source
   *                              fabric or only the essential version configs and generate the rest based on
   *                              destination fabric's Store configs.
   * @param sourceFabricVersion source fabric's Version configs used to configure the recovering version in the
   *                            destination fabric.
   */
  void initiateDataRecovery(
      String clusterName,
      String storeName,
      int version,
      String sourceFabric,
      String destinationFabric,
      boolean copyAllVersionConfigs,
      Optional<Version> sourceFabricVersion);

  /**
   * Prepare for data recovery in the destination fabric. The interested store version might have lingering states
   * and resources in the destination fabric from previous failed attempts. Perform some basic checks to make sure
   * the store version in the destination fabric is capable of performing data recovery and cleanup any lingering
   * states and resources.
   */
  void prepareDataRecovery(
      String clusterName,
      String storeName,
      int version,
      String sourceFabric,
      String destinationFabric,
      Optional<Integer> sourceAmplificationFactor);

  /**
   * Check if the store version's previous states and resources are cleaned up and ready to start data recovery.
   * @return whether is ready to start data recovery and the reason if it's not ready.
   */
  Pair<Boolean, String> isStoreVersionReadyForDataRecovery(
      String clusterName,
      String storeName,
      int version,
      String sourceFabric,
      String destinationFabric,
      Optional<Integer> sourceAmplificationFactor);

  /**
   * Return whether the admin consumption task is enabled for the passed cluster.
   */
  default boolean isAdminTopicConsumptionEnabled(String clusterName) {
    return true;
  }

  /**
   * Return all stores in a cluster.
   */
  ArrayList<StoreInfo> getClusterStores(String clusterName);

  Map<String, StoreDataAudit> getClusterStaleStores(String clusterName);

  /**
   * @return the largest used version number for the given store from store graveyard.
   */
  int getLargestUsedVersionFromStoreGraveyard(String clusterName, String storeName);

  Map<String, RegionPushDetails> listStorePushInfo(
      String clusterName,
      String storeName,
      boolean isPartitionDetailEnabled);

  RegionPushDetails getRegionPushDetails(String clusterName, String storeName, boolean isPartitionDetailEnabled);

  Map<String, Long> getAdminTopicMetadata(String clusterName, Optional<String> storeName);

  void updateAdminTopicMetadata(
      String clusterName,
      long executionId,
      Optional<String> storeName,
      Optional<Long> offset,
      Optional<Long> upstreamOffset);

  void createStoragePersona(
      String clusterName,
      String name,
      long quotaNumber,
      Set<String> storesToEnforce,
      Set<String> owners);

  StoragePersona getStoragePersona(String clusterName, String name);

  void deleteStoragePersona(String clusterName, String name);

  void updateStoragePersona(String clusterName, String name, UpdateStoragePersonaQueryParams queryParams);

  StoragePersona getPersonaAssociatedWithStore(String clusterName, String storeName);

  List<StoragePersona> getClusterStoragePersonas(String clusterName);

  /**
   * Scan through instance level customized states and remove any lingering ZNodes that are no longer relevant. This
   * operation shouldn't be needed under normal circumstances. It's intended to cleanup ZNodes that failed to be deleted
   * due to bugs and errors.
   * @param clusterName to perform the cleanup.
   * @return list of deleted ZNode paths.
   */
  List<String> cleanupInstanceCustomizedStates(String clusterName);

  StoreGraveyard getStoreGraveyard();

  void removeStoreFromGraveyard(String clusterName, String storeName);

  default void startInstanceMonitor(String clusterName) {
  }

  default void clearInstanceMonitor(String clusterName) {
  }

  PushStatusStoreReader getPushStatusStoreReader();

  PushStatusStoreWriter getPushStatusStoreWriter();

  /**
   * Send a heartbeat timestamp to targeted system store.
   */
  void sendHeartbeatToSystemStore(String clusterName, String storeName, long heartbeatTimestamp);

  /**
   * Read the latest heartbeat timestamp from system store. If it failed to read from system store, this method should return -1.
   */
  long getHeartbeatFromSystemStore(String clusterName, String storeName);

  /**
   * @return the aggregate resources required by controller to manage a Venice cluster.
   */
  HelixVeniceClusterResources getHelixVeniceClusterResources(String cluster);

  PubSubTopicRepository getPubSubTopicRepository();
}
