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
import com.linkedin.venice.controllerapi.MultiVersionStatusResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.request.AdminCommandExecutionStatusRequest;
import com.linkedin.venice.controllerapi.request.AdminTopicMetadataRequest;
import com.linkedin.venice.controllerapi.request.ClusterDiscoveryRequest;
import com.linkedin.venice.controllerapi.request.ControllerRequest;
import com.linkedin.venice.controllerapi.request.NewStoreRequest;
import com.linkedin.venice.controllerapi.request.UpdateAclForStoreRequest;
import com.linkedin.venice.controllerapi.request.UpdateAdminTopicMetadataRequest;
import com.linkedin.venice.controllerapi.routes.AdminCommandExecutionResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.utils.Pair;
import java.util.Map;
import java.util.Optional;
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
}
