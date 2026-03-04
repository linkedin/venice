package com.linkedin.venice.controller.server;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.ControllerRequestHandlerDependencies;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.protocols.controller.ClusterStoreGrpcInfo;
import com.linkedin.venice.protocols.controller.DiscoverClusterGrpcRequest;
import com.linkedin.venice.protocols.controller.DiscoverClusterGrpcResponse;
import com.linkedin.venice.protocols.controller.GetFutureVersionGrpcRequest;
import com.linkedin.venice.protocols.controller.GetFutureVersionGrpcResponse;
import com.linkedin.venice.protocols.controller.LeaderControllerGrpcRequest;
import com.linkedin.venice.protocols.controller.LeaderControllerGrpcResponse;
import com.linkedin.venice.utils.Pair;
import java.util.Collections;
import java.util.Map;
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
  private final Admin admin;
  private final boolean sslEnabled;
  private final VeniceControllerAccessManager accessManager;
  private final StoreRequestHandler storeRequestHandler;
  private final ClusterAdminOpsRequestHandler clusterAdminOpsRequestHandler;
  private final SchemaRequestHandler schemaRequestHandler;

  public VeniceControllerRequestHandler(ControllerRequestHandlerDependencies dependencies) {
    this.admin = dependencies.getAdmin();
    this.sslEnabled = dependencies.isSslEnabled();
    this.accessManager = dependencies.getControllerAccessManager();
    this.storeRequestHandler = new StoreRequestHandler(dependencies);
    this.clusterAdminOpsRequestHandler = new ClusterAdminOpsRequestHandler(dependencies);
    this.schemaRequestHandler = new SchemaRequestHandler(dependencies);
  }

  // visibility: package-private
  boolean isSslEnabled() {
    return sslEnabled;
  }

  public Admin getAdmin() {
    return admin;
  }

  public StoreRequestHandler getStoreRequestHandler() {
    return storeRequestHandler;
  }

  public ClusterAdminOpsRequestHandler getClusterAdminOpsRequestHandler() {
    return clusterAdminOpsRequestHandler;
  }

  public SchemaRequestHandler getSchemaRequestHandler() {
    return schemaRequestHandler;
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
    String clusterName = admin.discoverCluster(storeName);

    DiscoverClusterGrpcResponse.Builder responseBuilder =
        DiscoverClusterGrpcResponse.newBuilder().setStoreName(storeName);
    if (clusterName != null) {
      responseBuilder.setClusterName(clusterName);
    }
    String routerD2Service = admin.getRouterD2Service(clusterName);
    if (routerD2Service != null) {
      responseBuilder.setD2Service(routerD2Service);
    }
    String serverD2Service = admin.getServerD2Service(clusterName);
    if (serverD2Service != null) {
      responseBuilder.setServerD2Service(serverD2Service);
    }
    return responseBuilder.build();
  }

  public VeniceControllerAccessManager getControllerAccessManager() {
    return accessManager;
  }

  /**
   * Gets the future version for a store in a cluster.
   * For parent controllers, returns future versions for all colos.
   * For child controllers, returns the local future version.
   * @param request the request containing cluster and store name
   * @return response containing a map of store/region to future version
   */
  public GetFutureVersionGrpcResponse getFutureVersion(GetFutureVersionGrpcRequest request) {
    ClusterStoreGrpcInfo storeInfo = request.getStoreInfo();
    ControllerRequestParamValidator.validateClusterStoreInfo(storeInfo);
    String clusterName = storeInfo.getClusterName();
    String storeName = storeInfo.getStoreName();

    LOGGER.info("Getting future version for store: {} in cluster: {}", storeName, clusterName);

    Store store = admin.getStore(clusterName, storeName);
    if (store == null) {
      throw new VeniceNoStoreException(storeName);
    }

    Map<String, String> storeVersionMap = admin.getFutureVersionsForMultiColos(clusterName, storeName);
    if (storeVersionMap.isEmpty()) {
      // Non parent controllers will return an empty map, so we'll just return the child version of this api
      storeVersionMap =
          Collections.singletonMap(storeName, String.valueOf(admin.getFutureVersion(clusterName, storeName)));
    }

    return GetFutureVersionGrpcResponse.newBuilder()
        .setStoreInfo(storeInfo)
        .putAllStoreVersionMap(storeVersionMap)
        .build();
  }
}
