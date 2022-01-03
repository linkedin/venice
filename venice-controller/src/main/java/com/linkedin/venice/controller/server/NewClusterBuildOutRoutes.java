package com.linkedin.venice.controller.server;

import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.ControllerResponse;
import java.util.Optional;
import spark.Request;
import spark.Route;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.*;
import static com.linkedin.venice.controllerapi.ControllerRoute.*;


public class NewClusterBuildOutRoutes extends AbstractRoute {
  public NewClusterBuildOutRoutes(Optional<DynamicAccessController> accessController) {
    super(accessController);
  }

  public Route copyOverStoresSchemasAndConfigs(Admin admin) {
    return new VeniceRouteHandler<ControllerResponse>(ControllerResponse.class) {
      @Override
      public void internalHandle(Request request, ControllerResponse veniceResponse) {
        // Only allow whitelist users to run this command
        if (!isWhitelistUsers(request)) {
          veniceResponse.setError("Only admin users are allowed to run " + request.url());
          return;
        }
        AdminSparkServer.validateParams(request, REPLICATE_META_DATA.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);
        String srcFabric = request.queryParams(SOURCE_FABRIC);
        String destFabric = request.queryParams(DEST_FABRIC);

        veniceResponse.setCluster(clusterName);
        admin.copyOverStoresSchemasAndConfigs(clusterName, srcFabric, destFabric);
      }
    };
  }
}
