package com.linkedin.venice.controller.server;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponse;
import com.linkedin.venice.utils.Pair;
import spark.Route;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.*;
import static com.linkedin.venice.controllerapi.ControllerRoute.*;


public class ClusterDiscovery {

  /**
   * No ACL check; any user is allowed to discover cluster
   */
  public static Route discoverCluster(Admin admin) {
    return (request, response) -> {
      D2ServiceDiscoveryResponse responseObject = new D2ServiceDiscoveryResponse();
      try {
        AdminSparkServer.validateParams(request, CLUSTER_DISCOVERY.getParams(), admin);
        responseObject.setName(request.queryParams(NAME));
        Pair<String, String> clusterToD2Pair = admin.discoverCluster(responseObject.getName());
        responseObject.setCluster(clusterToD2Pair.getFirst());
        responseObject.setD2Service(clusterToD2Pair.getSecond());
      } catch (Throwable e) {
        responseObject.setError(e);
        AdminSparkServer.handleError(e, request, response);
      }
      response.type(HttpConstants.JSON);
      return AdminSparkServer.mapper.writeValueAsString(responseObject);
    };
  }
}
