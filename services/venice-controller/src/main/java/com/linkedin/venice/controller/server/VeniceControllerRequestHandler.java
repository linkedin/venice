package com.linkedin.venice.controller.server;

import com.linkedin.venice.LastSucceedExecutionIdResponse;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.AdminCommandExecutionTracker;
import com.linkedin.venice.controller.AdminTopicMetadataAccessor;
import com.linkedin.venice.controller.ControllerRequestHandlerDependencies;
import com.linkedin.venice.controllerapi.AclResponse;
import com.linkedin.venice.controllerapi.AdminCommandExecution;
import com.linkedin.venice.controllerapi.AdminTopicMetadataResponse;
import com.linkedin.venice.controllerapi.ControllerEndpointParamValidator;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponse;
import com.linkedin.venice.controllerapi.LeaderControllerResponse;
import com.linkedin.venice.controllerapi.MultiStoreResponse;
import com.linkedin.venice.controllerapi.MultiVersionStatusResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.controllerapi.request.AdminCommandExecutionStatusRequest;
import com.linkedin.venice.controllerapi.request.AdminTopicMetadataRequest;
import com.linkedin.venice.controllerapi.request.ClusterDiscoveryRequest;
import com.linkedin.venice.controllerapi.request.ControllerRequest;
import com.linkedin.venice.controllerapi.request.EmptyPushRequest;
import com.linkedin.venice.controllerapi.request.GetStoreRequest;
import com.linkedin.venice.controllerapi.request.ListStoresRequest;
import com.linkedin.venice.controllerapi.request.NewStoreRequest;
import com.linkedin.venice.controllerapi.request.UpdateAclForStoreRequest;
import com.linkedin.venice.controllerapi.request.UpdateAdminTopicMetadataRequest;
import com.linkedin.venice.controllerapi.routes.AdminCommandExecutionResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.exceptions.VeniceUnsupportedOperationException;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.ZKStore;
import com.linkedin.venice.utils.Pair;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * The core handler for processing incoming requests in the VeniceController.
 * Acts as the central entry point for handling requests received via both HTTP/REST and gRPC protocols.
 * This class is responsible for managing all request handling operations for the VeniceController.
 */
public class VeniceControllerRequestHandler {
  private static final Logger LOGGER = LogManager.getLogger(VeniceControllerGrpcServiceImpl.class);
  private final Admin admin;
  private final boolean sslEnabled;

  public static final String DEFAULT_STORE_OWNER = "";

  public VeniceControllerRequestHandler(ControllerRequestHandlerDependencies dependencies) {
    this.admin = dependencies.getAdmin();
    this.sslEnabled = dependencies.isSslEnabled();
  }

  public void getLeaderController(String clusterName, LeaderControllerResponse response) {
    Instance leaderController = admin.getLeaderController(clusterName);
    response.setCluster(clusterName);
    response.setUrl(leaderController.getUrl(isSslEnabled()));
    if (leaderController.getPort() != leaderController.getSslPort()) {
      // Controller is SSL Enabled
      response.setSecureUrl(leaderController.getUrl(true));
    }
    response.setGrpcUrl(leaderController.getGrpcUrl());
    response.setSecureGrpcUrl(leaderController.getGrpcSslUrl());
  }

  // visibility: package-private
  boolean isSslEnabled() {
    return sslEnabled;
  }

  /**
   * Creates a new store in the specified Venice cluster with the provided parameters.
   * @param request the request object containing all necessary details for the creation of the store
   */
  public void createStore(NewStoreRequest request, NewStoreResponse response) {
    ControllerEndpointParamValidator.validateNewStoreRequest(request);

    String clusterName = request.getClusterName();
    String storeName = request.getStoreName();
    String keySchema = request.getKeySchema();
    String valueSchema = request.getValueSchema();
    String owner = request.getOwner() == null ? DEFAULT_STORE_OWNER : request.getOwner();
    Optional<String> accessPermissions = Optional.ofNullable(request.getAccessPermissions());
    boolean isSystemStore = request.isSystemStore();

    LOGGER.info(
        "Creating store: {} in cluster: {} with owner: {} and key schema: {} and value schema: {} and isSystemStore: {} and access permissions: {}",
        storeName,
        clusterName,
        owner,
        keySchema,
        valueSchema,
        isSystemStore,
        accessPermissions);

    admin.createStore(clusterName, storeName, owner, keySchema, valueSchema, isSystemStore, accessPermissions);

    response.setCluster(clusterName);
    response.setName(storeName);
    response.setOwner(owner);
    LOGGER.info("Successfully created store: {} in cluster: {}", storeName, clusterName);
  }

  public void updateAclForStore(UpdateAclForStoreRequest request, AclResponse response) {
    String cluster = request.getClusterName();
    String storeName = request.getStoreName();
    String accessPermissions = request.getAccessPermissions();
    LOGGER.info(
        "Updating ACL for store: {} in cluster: {} with access permissions: {}",
        storeName,
        cluster,
        accessPermissions);
    admin.updateAclForStore(cluster, storeName, accessPermissions);
    LOGGER.info("Successfully updated ACL for store: {} in cluster: {}", storeName, cluster);
    response.setCluster(cluster);
    response.setName(storeName);
  }

  public void getAclForStore(ControllerRequest request, AclResponse response) {
    String clusterName = request.getClusterName();
    String storeName = request.getStoreName();
    LOGGER.info("Getting ACL for store: {} in cluster: {}", storeName, clusterName);
    String accessPermissions = admin.getAclForStore(clusterName, storeName);
    response.setCluster(clusterName);
    response.setName(storeName);
    response.setAccessPermissions(accessPermissions);
  }

  public void deleteAclForStore(ControllerRequest request, AclResponse response) {
    String clusterName = request.getClusterName();
    String storeName = request.getStoreName();
    response.setCluster(clusterName);
    response.setName(storeName);
    LOGGER.info("Deleting ACL for store: {} in cluster: {}", storeName, clusterName);
    admin.deleteAclForStore(clusterName, storeName);
  }

  public void checkResourceCleanupBeforeStoreCreation(ControllerRequest request, ControllerResponse response) {
    String clusterName = request.getClusterName();
    String storeName = request.getStoreName();
    response.setCluster(clusterName);

    LOGGER.info("Checking resource cleanup before creating store: {} in cluster: {}", storeName, clusterName);
    admin.checkResourceCleanupBeforeStoreCreation(clusterName, storeName);
    response.setName(storeName);
  }

  public void getAdminCommandExecutionStatus(
      AdminCommandExecutionStatusRequest request,
      AdminCommandExecutionResponse response) {
    String clusterName = request.getClusterName();
    long executionId = request.getAdminCommandExecutionId();
    response.setCluster(clusterName);
    LOGGER.info("Getting admin command execution status for execution id: {} in cluster: {}", executionId, clusterName);
    Optional<AdminCommandExecutionTracker> adminCommandExecutionTracker =
        admin.getAdminCommandExecutionTracker(clusterName);
    if (!adminCommandExecutionTracker.isPresent()) {
      throw new VeniceException(
          "Could not track execution in this controller. Make sure you send the command to a correct parent controller.");
    }
    AdminCommandExecutionTracker tracker = adminCommandExecutionTracker.get();
    AdminCommandExecution execution = tracker.checkExecutionStatus(executionId);
    if (execution == null) {
      throw new VeniceException(
          "Could not find the execution by given id: " + executionId + " in cluster: " + clusterName);
    }
    response.setCluster(clusterName);
    response.setExecution(execution);
  }

  public void getLastSucceedExecutionId(ControllerRequest request, LastSucceedExecutionIdResponse response) {
    String clusterName = request.getClusterName();
    response.setCluster(clusterName);
    LOGGER.info("Getting last succeeded execution id in cluster: {}", clusterName);
    long lastSucceedExecutionId = admin.getLastSucceedExecutionId(clusterName);
    response.setLastSucceedExecutionId(lastSucceedExecutionId);
  }

  public void getClusterDiscovery(ClusterDiscoveryRequest request, D2ServiceDiscoveryResponse response) {
    String storeName = request.getStoreName();
    response.setName(storeName);
    LOGGER.info("Discovering cluster for store: {}", storeName);
    Pair<String, String> clusterToD2Pair = admin.discoverCluster(storeName);
    response.setCluster(clusterToD2Pair.getFirst());
    response.setD2Service(clusterToD2Pair.getSecond());
    response.setServerD2Service(admin.getServerD2Service(clusterToD2Pair.getFirst()));
  }

  public void listBootstrappingVersions(ControllerRequest request, MultiVersionStatusResponse response) {
    String clusterName = request.getClusterName();
    response.setCluster(clusterName);
    LOGGER.info("Listing bootstrapping versions for cluster: {}", clusterName);
    response.setVersionStatusMap(admin.findAllBootstrappingVersions(clusterName));
  }

  public void getAdminTopicMetadata(AdminTopicMetadataRequest request, AdminTopicMetadataResponse response) {
    String clusterName = request.getClusterName();
    String storeName = request.getStoreName();
    response.setCluster(clusterName);
    if (storeName != null) {
      response.setName(storeName);
    }
    LOGGER.info(
        "Getting admin topic metadata for cluster: {}{}",
        clusterName,
        storeName != null ? " and store: " + storeName : "");

    Map<String, Long> metadata = admin.getAdminTopicMetadata(clusterName, Optional.ofNullable(storeName));
    response.setExecutionId(AdminTopicMetadataAccessor.getExecutionId(metadata));
    if (storeName == null) {
      Pair<Long, Long> offsets = AdminTopicMetadataAccessor.getOffsets(metadata);
      response.setOffset(offsets.getFirst());
      response.setUpstreamOffset(offsets.getSecond());
    }
  }

  public void updateAdminTopicMetadata(UpdateAdminTopicMetadataRequest request, ControllerResponse response) {
    String clusterName = request.getClusterName();
    long executionId = request.getExecutionId();
    String storeName = request.getStoreName();
    Long offset = request.getOffset();
    Long upstreamOffset = request.getUpstreamOffset();

    response.setCluster(clusterName);
    if (storeName != null) {
      response.setName(storeName);
    }

    if (storeName != null) {
      if (offset != null || upstreamOffset != null) {
        throw new VeniceException("There is no store-level offsets to be updated");
      }
    } else {
      if (offset == null || upstreamOffset == null) {
        throw new VeniceException("Offsets must be provided to update cluster-level admin topic metadata");
      }
    }

    admin.updateAdminTopicMetadata(
        clusterName,
        executionId,
        Optional.ofNullable(storeName),
        Optional.ofNullable(offset),
        Optional.ofNullable(upstreamOffset));
  }

  public void listStores(ListStoresRequest request, MultiStoreResponse response) {
    String clusterName = request.getClusterName();
    boolean excludeSystemStores = request.isExcludeSystemStores();
    String storeConfigNameFilter = request.getStoreConfigNameFilter();
    String storeConfigValueFilter = request.getStoreConfigValueFilter();
    Schema.Field configFilterField = request.getConfigFilterField();
    boolean isDataReplicationPolicyConfigFilter = request.isDataReplicationPolicyConfigFilter();
    LOGGER.info(
        "Getting all stores for cluster: {} with excludeSystemStores: {} and storeConfigNameFilter: {} and storeConfigValueFilter: {}",
        clusterName,
        excludeSystemStores,
        storeConfigNameFilter,
        storeConfigValueFilter);

    response.setCluster(clusterName);

    List<Store> storeList = admin.getAllStores(clusterName);

    // If no system store or configuration filtering is applied, return all stores
    if (!excludeSystemStores && storeConfigNameFilter == null) {
      response.setStores(extractStoreNames(storeList));
      return;
    }

    List<Store> selectedStoreList = new ArrayList<>();
    for (Store store: storeList) {
      // Filter out system stores if requested
      if (excludeSystemStores && store.isSystemStore()) {
        continue;
      }
      // If store config filter is not present, add the store to the list and continue with the next store
      if (storeConfigNameFilter == null) {
        selectedStoreList.add(store);
        continue;
      }

      // If data replication policy filter is present, check if the store matches the filter
      if (isDataReplicationPolicyConfigFilter && matchDataReplicationPolicy(store, storeConfigValueFilter)) {
        selectedStoreList.add(store);
        continue;
      }

      // If the store config filter is present, check if the store matches the filter
      if (matchConfigValueForStore(store, storeConfigNameFilter, storeConfigValueFilter, configFilterField.schema())) {
        selectedStoreList.add(store);
      }
    }
    response.setStores(extractStoreNames(selectedStoreList));
  }

  public static String[] extractStoreNames(List<Store> storeList) {
    String[] storeNameList = new String[storeList.size()];
    for (int i = 0; i < storeList.size(); i++) {
      storeNameList[i] = storeList.get(i).getName();
    }
    return storeNameList;
  }

  public static boolean matchConfigValueForStore(
      Store store,
      String storeConfigNameFilter,
      String storeConfigValueFilter,
      Schema configFilterFieldSchema) {
    ZKStore cloneStore = new ZKStore(store);
    Object configValue = cloneStore.dataModel().get(storeConfigNameFilter);
    if (configValue == null) {
      // If the store doesn't have the config, it fails the match
      return false;
    }
    // Compare based on schema type
    return matchConfigValue(storeConfigValueFilter, configValue, configFilterFieldSchema);
  }

  // Utility method to check data replication policy matching
  public static boolean matchDataReplicationPolicy(Store store, String storeConfigValueFilter) {
    if (!store.isHybrid() || store.getHybridStoreConfig().getDataReplicationPolicy() == null) {
      // If the store is not hybrid or doesn't have a data replication policy, it fails the match
      return false;
    }
    return store.getHybridStoreConfig().getDataReplicationPolicy().name().equalsIgnoreCase(storeConfigValueFilter);
  }

  private static boolean matchConfigValue(String storeConfigValueFilter, Object configValue, Schema fieldSchema) {
    switch (fieldSchema.getType()) {
      case BOOLEAN:
        return Boolean.valueOf(storeConfigValueFilter).equals((Boolean) configValue);
      case INT:
        return Integer.valueOf(storeConfigValueFilter).equals((Integer) configValue);
      case LONG:
        return Long.valueOf(storeConfigValueFilter).equals((Long) configValue);
      case FLOAT:
        return Float.valueOf(storeConfigValueFilter).equals(configValue);
      case DOUBLE:
        return Double.valueOf(storeConfigValueFilter).equals(configValue);
      case STRING:
        return storeConfigValueFilter.equals(configValue);
      case ENUM:
        return storeConfigValueFilter.equals(configValue.toString());
      case UNION:
        return configValue != null;
      default:
        throw new IllegalArgumentException("Unsupported store config filter for type: " + fieldSchema.getType());
    }
  }

  public void getStore(GetStoreRequest request, StoreResponse response) {
    String clusterName = request.getClusterName();
    String storeName = request.getStoreName();
    response.setCluster(clusterName);
    response.setName(storeName);
    LOGGER.info("Getting store: {} in cluster: {}", storeName, clusterName);

    Store store = admin.getStore(clusterName, storeName);
    if (store == null) {
      throw new VeniceNoStoreException(storeName);
    }
    StoreInfo storeInfo = StoreInfo.fromStore(store);
    // Make sure store info will have right default retention time for Nuage UI display.
    if (storeInfo.getBackupVersionRetentionMs() < 0) {
      storeInfo.setBackupVersionRetentionMs(admin.getBackupVersionDefaultRetentionMs());
    }
    // This is the only place the default value of maxRecordSizeBytes is set for StoreResponse for VPJ and Consumer
    if (storeInfo.getMaxRecordSizeBytes() < 0) {
      storeInfo.setMaxRecordSizeBytes(admin.getDefaultMaxRecordSizeBytes());
    }
    storeInfo.setColoToCurrentVersions(admin.getCurrentVersionsForMultiColos(clusterName, storeName));
    boolean isSSL = admin.isSSLEnabledForPush(clusterName, storeName);
    storeInfo.setKafkaBrokerUrl(admin.getKafkaBootstrapServers(isSSL));

    response.setStore(storeInfo);
  }

  public void emptyPush(EmptyPushRequest request, VersionCreationResponse response) {
    String clusterName = request.getClusterName();
    String storeName = request.getStoreName();
    String pushJobId = request.getPushJobId();
    Version version = null;
    LOGGER.info("Empty push for store: {} in cluster: {} with push job id: {}", storeName, clusterName, pushJobId);
    try {
      if (!admin.whetherEnableBatchPushFromAdmin(storeName)) {
        throw new VeniceUnsupportedOperationException(
            "EMPTY PUSH",
            "Please push data to Venice Parent Colo instead or use Aggregate mode if you are running Samza GF Job.");
      }

      int partitionNum = admin.calculateNumberOfPartitions(clusterName, storeName);
      int replicationFactor = admin.getReplicationFactor(clusterName, storeName);
      version = admin.incrementVersionIdempotent(clusterName, storeName, pushJobId, partitionNum, replicationFactor);
      int versionNumber = version.getNumber();
      response.setCluster(clusterName);
      response.setName(storeName);
      response.setVersion(versionNumber);
      response.setPartitions(partitionNum);
      response.setReplicas(replicationFactor);
      response.setKafkaTopic(version.kafkaTopicName());
      response.setKafkaBootstrapServers(version.getPushStreamSourceAddress());

      admin.writeEndOfPush(clusterName, storeName, versionNumber, true);
    } catch (Throwable e) {
      // Clean up on failed push.
      if (version != null && clusterName != null) {
        LOGGER.warn(
            "Cleaning up failed Empty push: {} with storeVersion {} cluster: {}",
            pushJobId,
            clusterName,
            version.kafkaTopicName());
        admin.killOfflinePush(clusterName, version.kafkaTopicName(), true);
      }
      throw e;
    }
  }
}
