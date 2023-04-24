package com.linkedin.venice.controller.server;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.CLUSTER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.DEST_FABRIC;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.NAME;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.SOURCE_FABRIC;
import static com.linkedin.venice.controllerapi.ControllerRoute.REPLICATE_META_DATA;

import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.authentication.AuthenticationService;
import com.linkedin.venice.authorization.AuthorizerService;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.meta.StoreInfo;
import java.util.Optional;
import spark.Request;
import spark.Route;


public class NewClusterBuildOutRoutes extends AbstractRoute {
  public NewClusterBuildOutRoutes(
      boolean sslEnabled,
      Optional<DynamicAccessController> accessController,
      Optional<AuthenticationService> authenticationService,
      Optional<AuthorizerService> authorizerService) {
    super(sslEnabled, accessController, authenticationService, authorizerService);
  }

  /**
   * @see Admin#copyOverStoreSchemasAndConfigs(String, String, String, String)
   */
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
