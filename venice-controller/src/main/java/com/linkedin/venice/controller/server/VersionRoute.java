package com.linkedin.venice.controller.server;

import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.MultiVersionStatusResponse;
import java.util.Optional;
import spark.Request;
import spark.Route;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.CLUSTER;
import static com.linkedin.venice.controllerapi.ControllerRoute.LIST_BOOTSTRAPPING_VERSIONS;


public class VersionRoute extends AbstractRoute {
  public VersionRoute(Optional<DynamicAccessController> accessController) {
    super(accessController);
  }

  public Route listBootstrappingVersions(Admin admin) {
    return new VeniceRouteHandler<MultiVersionStatusResponse>(MultiVersionStatusResponse.class) {

      @Override
      public void internalHandle(Request request, MultiVersionStatusResponse veniceResponse) {
        // Only allow whitelist users to run this command
        if (!isWhitelistUsers(request)) {
          veniceResponse.setError("Only admin users are allowed to run " + request.url());
          return;
        }
        AdminSparkServer.validateParams(request, LIST_BOOTSTRAPPING_VERSIONS.getParams(), admin);
        String cluster = request.queryParams(CLUSTER);
        veniceResponse.setCluster(cluster);
        veniceResponse.setVersionStatusMap(admin.findAllBootstrappingVersions(cluster));
      }
    };
  }
}
