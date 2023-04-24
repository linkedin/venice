package com.linkedin.venice.controller.server;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.CLUSTER;
import static com.linkedin.venice.controllerapi.ControllerRoute.LIST_BOOTSTRAPPING_VERSIONS;

import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.authentication.AuthenticationService;
import com.linkedin.venice.authorization.AuthorizerService;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.MultiVersionStatusResponse;
import java.util.Optional;
import spark.Request;
import spark.Route;


public class VersionRoute extends AbstractRoute {
  public VersionRoute(
      boolean sslEnabled,
      Optional<DynamicAccessController> accessController,
      Optional<AuthenticationService> authenticationService,
      Optional<AuthorizerService> authorizerService) {
    super(sslEnabled, accessController, authenticationService, authorizerService);
  }

  /**
   * No ACL check; any user is allowed to check bootstrapping versions.
   * @see Admin#findAllBootstrappingVersions(String)
   */
  public Route listBootstrappingVersions(Admin admin) {
    return new VeniceRouteHandler<MultiVersionStatusResponse>(MultiVersionStatusResponse.class) {
      @Override
      public void internalHandle(Request request, MultiVersionStatusResponse veniceResponse) {
        AdminSparkServer.validateParams(request, LIST_BOOTSTRAPPING_VERSIONS.getParams(), admin);
        String cluster = request.queryParams(CLUSTER);
        veniceResponse.setCluster(cluster);
        veniceResponse.setVersionStatusMap(admin.findAllBootstrappingVersions(cluster));
      }
    };
  }
}
