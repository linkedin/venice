package com.linkedin.venice.controller.server;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.MultiVersionStatusResponse;
import spark.Request;
import spark.Route;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.CLUSTER;
import static com.linkedin.venice.controllerapi.ControllerRoute.LIST_BOOTSTRAPPING_VERSIONS;


public class VersionRoute {
  public static Route listBootstrappingVersions(Admin admin) {
    return new VeniceRouteHandler<MultiVersionStatusResponse>(MultiVersionStatusResponse.class) {

      @Override
      public void internalHandle(Request request, MultiVersionStatusResponse veniceRepsonse) {
        AdminSparkServer.validateParams(request, LIST_BOOTSTRAPPING_VERSIONS.getParams(), admin);
        String cluster = request.queryParams(CLUSTER);
        veniceRepsonse.setCluster(cluster);
        veniceRepsonse.setVersionStatusMap(admin.findAllBootstrappingVersions(cluster));
      }
    };
  }
}
