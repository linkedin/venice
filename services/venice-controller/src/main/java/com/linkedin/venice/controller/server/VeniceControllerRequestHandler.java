package com.linkedin.venice.controller.server;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.ControllerRequestHandlerDependencies;
import com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponse;
import com.linkedin.venice.controllerapi.LeaderControllerResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.request.ClusterDiscoveryRequest;
import com.linkedin.venice.controllerapi.request.ControllerRequest;
import com.linkedin.venice.controllerapi.request.CreateNewStoreRequest;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.utils.Pair;
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

  public VeniceControllerRequestHandler(ControllerRequestHandlerDependencies dependencies) {
    this.admin = dependencies.getAdmin();
    this.sslEnabled = dependencies.isSslEnabled();
  }

  // visibility: package-private
  boolean isSslEnabled() {
    return sslEnabled;
  }

  /**
   * The response is passed as an argument to avoid creating duplicate response objects for HTTP requests
   * and to simplify unit testing with gRPC. Once the transition to gRPC is complete, we can eliminate
   * the need to pass the response as an argument and instead construct and return it directly within the method.
   */
  public void getLeaderController(ControllerRequest request, LeaderControllerResponse response) {
    String clusterName = request.getClusterName();
    response.setCluster(clusterName);

    Instance leaderControllerInstance = admin.getLeaderController(clusterName);
    response.setUrl(leaderControllerInstance.getUrl(isSslEnabled()));
    if (leaderControllerInstance.getPort() != leaderControllerInstance.getSslPort()) {
      // Controller is SSL Enabled
      response.setSecureUrl(leaderControllerInstance.getUrl(true));
    }
    response.setGrpcUrl(leaderControllerInstance.getGrpcUrl());
    response.setSecureGrpcUrl(leaderControllerInstance.getGrpcSslUrl());
  }

  public void discoverCluster(ClusterDiscoveryRequest request, D2ServiceDiscoveryResponse response) {
    String storeName = request.getStoreName();
    LOGGER.info("Discovering cluster for store: {}", storeName);
    Pair<String, String> clusterToD2Pair = admin.discoverCluster(storeName);
    response.setName(storeName);
    response.setCluster(clusterToD2Pair.getFirst());
    response.setD2Service(clusterToD2Pair.getSecond());
    response.setServerD2Service(admin.getServerD2Service(clusterToD2Pair.getFirst()));
  }

  /**
   * Creates a new store in the specified Venice cluster with the provided parameters.
   * @param request the request object containing all necessary details for the creation of the store
   */
  public void createStore(CreateNewStoreRequest request, NewStoreResponse response) {
    String clusterName = request.getClusterName();
    String storeName = request.getStoreName();
    String keySchema = request.getKeySchema();
    String valueSchema = request.getValueSchema();
    String owner = request.getOwner();
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
}
