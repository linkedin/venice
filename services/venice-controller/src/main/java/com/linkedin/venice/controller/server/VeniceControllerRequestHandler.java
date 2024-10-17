package com.linkedin.venice.controller.server;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.ControllerRequestHandlerDependencies;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.protocols.controller.ClusterStoreGrpcInfo;
import com.linkedin.venice.protocols.controller.CreateStoreGrpcRequest;
import com.linkedin.venice.protocols.controller.CreateStoreGrpcResponse;
import com.linkedin.venice.protocols.controller.DiscoverClusterGrpcRequest;
import com.linkedin.venice.protocols.controller.DiscoverClusterGrpcResponse;
import com.linkedin.venice.protocols.controller.LeaderControllerGrpcRequest;
import com.linkedin.venice.protocols.controller.LeaderControllerGrpcResponse;
import com.linkedin.venice.utils.Pair;
import java.util.Optional;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * The core handler for processing incoming requests in the VeniceController.
 * Acts as the central entry point for handling requests received via both HTTP/REST and gRPC protocols.
 * This class is responsible for managing all request handling operations for the VeniceController.
 */
public class VeniceControllerRequestHandler {
  private static final Logger LOGGER = LogManager.getLogger(VeniceControllerRequestHandler.class);
  public static final String DEFAULT_STORE_OWNER = "";
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
  public LeaderControllerGrpcResponse getLeaderControllerDetails(LeaderControllerGrpcRequest request) {
    String clusterName = request.getClusterName();
    if (StringUtils.isBlank(clusterName)) {
      throw new IllegalArgumentException("Cluster name is required for leader controller discovery");
    }
    Instance leaderControllerInstance = admin.getLeaderController(clusterName);
    String leaderControllerUrl = leaderControllerInstance.getUrl(isSslEnabled());
    String leaderControllerSecureUrl = null;
    if (leaderControllerInstance.getPort() != leaderControllerInstance.getSslPort()) {
      // Controller is SSL Enabled
      leaderControllerSecureUrl = leaderControllerInstance.getUrl(true);
    }
    LeaderControllerGrpcResponse.Builder responseBuilder =
        LeaderControllerGrpcResponse.newBuilder().setClusterName(clusterName);
    if (leaderControllerUrl != null) {
      responseBuilder.setHttpUrl(leaderControllerUrl);
    }
    if (leaderControllerSecureUrl != null) {
      responseBuilder.setHttpsUrl(leaderControllerSecureUrl);
    }
    String grpcUrl = leaderControllerInstance.getGrpcUrl();
    String secureGrpcUrl = leaderControllerInstance.getGrpcSslUrl();
    if (grpcUrl != null) {
      responseBuilder.setGrpcUrl(grpcUrl);
    }
    if (secureGrpcUrl != null) {
      responseBuilder.setSecureGrpcUrl(secureGrpcUrl);
    }
    return responseBuilder.build();
  }

  public DiscoverClusterGrpcResponse discoverCluster(DiscoverClusterGrpcRequest request) {
    String storeName = request.getStoreName();
    if (StringUtils.isBlank(storeName)) {
      throw new IllegalArgumentException("Store name is required for cluster discovery");
    }
    LOGGER.info("Discovering cluster for store: {}", storeName);
    Pair<String, String> clusterToD2Pair = admin.discoverCluster(storeName);

    DiscoverClusterGrpcResponse.Builder responseBuilder =
        DiscoverClusterGrpcResponse.newBuilder().setStoreName(storeName);
    if (clusterToD2Pair.getFirst() != null) {
      responseBuilder.setClusterName(clusterToD2Pair.getFirst());
    }
    if (clusterToD2Pair.getSecond() != null) {
      responseBuilder.setD2Service(clusterToD2Pair.getSecond());
    }
    String serverD2Service = admin.getServerD2Service(clusterToD2Pair.getFirst());
    if (serverD2Service != null) {
      responseBuilder.setServerD2Service(serverD2Service);
    }
    return responseBuilder.build();
  }

  /**
   * Creates a new store in the specified Venice cluster with the provided parameters.
   * @param request the request object containing all necessary details for the creation of the store
   */
  public CreateStoreGrpcResponse createStore(CreateStoreGrpcRequest request) {
    ClusterStoreGrpcInfo clusterStoreInfo = request.getClusterStoreInfo();
    String clusterName = clusterStoreInfo.getClusterName();
    String storeName = clusterStoreInfo.getStoreName();
    String keySchema = request.getKeySchema();
    String valueSchema = request.getValueSchema();
    String owner = request.hasOwner() ? request.getOwner() : null;
    if (owner == null) {
      owner = DEFAULT_STORE_OWNER;
    }
    Optional<String> accessPermissions =
        Optional.ofNullable(request.hasAccessPermission() ? request.getAccessPermission() : null);
    boolean isSystemStore = request.hasIsSystemStore() && request.getIsSystemStore();
    ControllerRequestParamValidator.createStoreRequestValidator(clusterName, storeName, owner, keySchema, valueSchema);
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
    CreateStoreGrpcResponse.Builder responseBuilder =
        CreateStoreGrpcResponse.newBuilder().setClusterStoreInfo(clusterStoreInfo).setOwner(owner);

    LOGGER.info("Successfully created store: {} in cluster: {}", storeName, clusterName);
    return responseBuilder.build();
  }
}
