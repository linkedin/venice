package com.linkedin.venice.controller.server;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.NAME;
import static com.linkedin.venice.controllerapi.ControllerRoute.CLUSTER_DISCOVERY;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponse;
import com.linkedin.venice.controllerapi.request.ClusterDiscoveryRequest;
import spark.Route;


public class ClusterDiscovery {
  /**
   * No ACL check; any user is allowed to discover cluster
   */
  public static Route discoverCluster(VeniceControllerRequestHandler requestHandler, Admin admin) {
    return (request, response) -> {
      D2ServiceDiscoveryResponse responseObject = new D2ServiceDiscoveryResponse();
      try {
        AdminSparkServer.validateParams(request, CLUSTER_DISCOVERY.getParams(), admin);
        requestHandler.getClusterDiscovery(new ClusterDiscoveryRequest(request.queryParams(NAME)), responseObject);
      } catch (Throwable e) {
        responseObject.setError(e);
        AdminSparkServer.handleError(e, request, response);
      }
      response.type(HttpConstants.JSON);
      return AdminSparkServer.OBJECT_MAPPER.writeValueAsString(responseObject);
    };
  }
}
