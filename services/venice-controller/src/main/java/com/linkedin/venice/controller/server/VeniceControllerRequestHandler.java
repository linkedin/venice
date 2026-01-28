package com.linkedin.venice.controller.server;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.ControllerRequestHandlerDependencies;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.protocols.controller.DiscoverClusterGrpcRequest;
import com.linkedin.venice.protocols.controller.DiscoverClusterGrpcResponse;
import com.linkedin.venice.protocols.controller.LeaderControllerGrpcRequest;
import com.linkedin.venice.protocols.controller.LeaderControllerGrpcResponse;
import com.linkedin.venice.protocols.controller.ListChildClustersGrpcRequest;
import com.linkedin.venice.protocols.controller.ListChildClustersGrpcResponse;
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
   * Lists all child clusters for a parent controller in a multi-cluster setup.
   * Returns empty maps for child controllers.
   * @param request the request containing cluster name
   * @return response containing child cluster controller URLs and D2 mappings
   */
  public ListChildClustersGrpcResponse listChildClusters(ListChildClustersGrpcRequest request) {
    String clusterName = request.getClusterName();
    if (StringUtils.isBlank(clusterName)) {
      throw new IllegalArgumentException("Cluster name is required");
    }

    LOGGER.info("Listing child clusters for cluster: {}", clusterName);

    ListChildClustersGrpcResponse.Builder responseBuilder =
        ListChildClustersGrpcResponse.newBuilder().setClusterName(clusterName);

    if (admin.isParent()) {
      Map<String, String> childUrlMap = admin.getChildDataCenterControllerUrlMap(clusterName);
      if (childUrlMap != null) {
        responseBuilder.putAllChildDataCenterControllerUrlMap(childUrlMap);
      }

      Map<String, String> childD2Map = admin.getChildDataCenterControllerD2Map(clusterName);
      if (childD2Map != null) {
        responseBuilder.putAllChildDataCenterControllerD2Map(childD2Map);
      }

      String d2ServiceName = admin.getChildControllerD2ServiceName(clusterName);
      if (d2ServiceName != null) {
        responseBuilder.setD2ServiceName(d2ServiceName);
      }
    }

    return responseBuilder.build();
  }
}
