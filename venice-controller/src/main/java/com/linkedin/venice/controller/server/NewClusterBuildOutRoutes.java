package com.linkedin.venice.controller.server;

import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.meta.StoreInfo;
import java.util.Optional;
import spark.Request;
import spark.Route;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.*;
import static com.linkedin.venice.controllerapi.ControllerRoute.*;


public class NewClusterBuildOutRoutes extends AbstractRoute {
  public NewClusterBuildOutRoutes(Optional<DynamicAccessController> accessController) {
    super(accessController);
  }

  public Route copyOverStoreSchemasAndConfigs(Admin admin) {
    return new VeniceRouteHandler<StoreResponse>(StoreResponse.class) {
      @Override
      public void internalHandle(Request request, StoreResponse veniceResponse) {
        // Only allow allowlist users to run this command
        if (!checkIsAllowListUser(request, veniceResponse, () -> isAllowListUser(request))) {
          return;
        }
        AdminSparkServer.validateParams(request, REPLICATE_META_DATA.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);
        String srcFabric = request.queryParams(SOURCE_FABRIC);
        String destFabric = request.queryParams(DEST_FABRIC);
        String storeName = request.queryParams(NAME);

        veniceResponse.setCluster(clusterName);
        veniceResponse.setName(storeName);
        StoreInfo storeInfo = admin.copyOverStoreSchemasAndConfigs(clusterName, srcFabric, destFabric, storeName);
        veniceResponse.setStore(storeInfo);
      }
    };
  }
}
