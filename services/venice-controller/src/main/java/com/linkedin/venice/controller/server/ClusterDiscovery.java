package com.linkedin.venice.controller.server;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.STORE_NAME;
import static com.linkedin.venice.controllerapi.ControllerRoute.CLUSTER_DISCOVERY;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponse;
import com.linkedin.venice.protocols.controller.DiscoverClusterGrpcRequest;
import com.linkedin.venice.protocols.controller.DiscoverClusterGrpcResponse;
import spark.Route;


public class ClusterDiscovery {
  /**
   * No ACL check; any user is allowed to discover cluster
   */
  public static Route discoverCluster(Admin admin, VeniceControllerRequestHandler requestHandler) {
    return (request, response) -> {
      D2ServiceDiscoveryResponse responseObject = new D2ServiceDiscoveryResponse();
      try {
        AdminSparkServer.validateParams(request, CLUSTER_DISCOVERY.getParams(), admin);
        DiscoverClusterGrpcResponse internalResponse = requestHandler.discoverCluster(
            DiscoverClusterGrpcRequest.newBuilder().setStoreName(request.queryParams(STORE_NAME)).build());
        responseObject.setName(internalResponse.getStoreName());
        responseObject.setCluster(internalResponse.getClusterName());
        responseObject.setD2Service(internalResponse.getD2Service());
        responseObject.setServerD2Service(internalResponse.getServerD2Service());
      } catch (Throwable e) {
        responseObject.setError(e);
        AdminSparkServer.handleError(e, request, response);
      }
      response.type(HttpConstants.JSON);
      return AdminSparkServer.OBJECT_MAPPER.writeValueAsString(responseObject);
    };
  }
}
