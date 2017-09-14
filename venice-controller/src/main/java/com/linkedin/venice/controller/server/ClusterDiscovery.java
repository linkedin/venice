package com.linkedin.venice.controller.server;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponse;
import com.linkedin.venice.utils.Pair;
import spark.Request;
import spark.Route;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.*;
import static com.linkedin.venice.controllerapi.ControllerRoute.*;


public class ClusterDiscovery {

  public static Route discoverCluster(Admin admin) {
    return new VeniceRouteHandler<D2ServiceDiscoveryResponse>(D2ServiceDiscoveryResponse.class) {

      @Override
      public void internalHandle(Request request, D2ServiceDiscoveryResponse veniceRepsonse) {
        AdminSparkServer.validateParams(request, CLUSTER_DISCOVERY.getParams(), admin);
        veniceRepsonse.setName(request.queryParams(NAME));
        Pair<String, String> clusterToD2Pair = admin.discoverCluster(veniceRepsonse.getName());
        veniceRepsonse.setCluster(clusterToD2Pair.getFirst());
        veniceRepsonse.setD2Service(clusterToD2Pair.getSecond());
      }
    };
  }
}
