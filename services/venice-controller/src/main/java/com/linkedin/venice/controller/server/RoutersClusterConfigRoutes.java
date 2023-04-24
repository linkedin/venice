package com.linkedin.venice.controller.server;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.CLUSTER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.EXPECTED_ROUTER_COUNT;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.STATUS;
import static com.linkedin.venice.controllerapi.ControllerRoute.ENABLE_MAX_CAPACITY_PROTECTION;
import static com.linkedin.venice.controllerapi.ControllerRoute.ENABLE_QUOTA_REBALANCED;
import static com.linkedin.venice.controllerapi.ControllerRoute.ENABLE_THROTTLING;
import static com.linkedin.venice.controllerapi.ControllerRoute.GET_ROUTERS_CLUSTER_CONFIG;

import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.authentication.AuthenticationService;
import com.linkedin.venice.authorization.AuthorizerService;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.RoutersClusterConfigResponse;
import com.linkedin.venice.utils.Utils;
import java.util.Optional;
import spark.Request;
import spark.Route;


public class RoutersClusterConfigRoutes extends AbstractRoute {
  public RoutersClusterConfigRoutes(
      boolean sslEnabled,
      Optional<DynamicAccessController> accessController,
      Optional<AuthenticationService> authenticationService,
      Optional<AuthorizerService> authorizerService) {
    super(sslEnabled, accessController, authenticationService, authorizerService);
  }

  /**
   * Enable throttling by updating the cluster level for all routers.
   */
  public Route enableThrottling(Admin admin) {
    return new VeniceRouteHandler<ControllerResponse>(ControllerResponse.class) {
      @Override
      public void internalHandle(Request request, ControllerResponse veniceResponse) {
        // Only allow allowlist users to run this command
        if (!checkIsAllowListUser(request, veniceResponse, () -> isAllowListUser(request))) {
          return;
        }
        AdminSparkServer.validateParams(request, ENABLE_THROTTLING.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);
        veniceResponse.setCluster(clusterName);
        boolean status = Utils.parseBooleanFromString(request.queryParams(STATUS), "enableThrottling");
        admin.updateRoutersClusterConfig(
            clusterName,
            Optional.of(status),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());
      }
    };
  }

  /**
   * Enable max capacity protection by updating the cluster level for all routers.
   */
  public Route enableMaxCapacityProtection(Admin admin) {
    return new VeniceRouteHandler<ControllerResponse>(ControllerResponse.class) {
      @Override
      public void internalHandle(Request request, ControllerResponse veniceResponse) {
        // Only allow allowlist users to run this command
        if (!checkIsAllowListUser(request, veniceResponse, () -> isAllowListUser(request))) {
          return;
        }
        AdminSparkServer.validateParams(request, ENABLE_MAX_CAPACITY_PROTECTION.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);
        veniceResponse.setCluster(clusterName);
        boolean status = Utils.parseBooleanFromString(request.queryParams(STATUS), "enableMaxCapacityProtection");
        admin.updateRoutersClusterConfig(
            clusterName,
            Optional.empty(),
            Optional.empty(),
            Optional.of(status),
            Optional.empty());
      }
    };
  }

  /**
   * Enable quota rebalanced by updating the cluster level for all routers.
   */
  public Route enableQuotaRebalanced(Admin admin) {
    return new VeniceRouteHandler<ControllerResponse>(ControllerResponse.class) {
      @Override
      public void internalHandle(Request request, ControllerResponse veniceResponse) {
        // Only allow allowlist users to run this command
        if (!checkIsAllowListUser(request, veniceResponse, () -> isAllowListUser(request))) {
          return;
        }
        AdminSparkServer.validateParams(request, ENABLE_QUOTA_REBALANCED.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);
        veniceResponse.setCluster(clusterName);
        boolean status = Utils.parseBooleanFromString(request.queryParams(STATUS), "enableQuotaRebalance");
        int expectedRouterCount =
            Utils.parseIntFromString(request.queryParams(EXPECTED_ROUTER_COUNT), "expectedRouterCount");
        admin.updateRoutersClusterConfig(
            clusterName,
            Optional.empty(),
            Optional.of(status),
            Optional.empty(),
            Optional.of(expectedRouterCount));
      }
    };
  }

  /**
   * No ACL check; any user is allowed to check router cluster configs.
   */
  public Route getRoutersClusterConfig(Admin admin) {
    return new VeniceRouteHandler<RoutersClusterConfigResponse>(RoutersClusterConfigResponse.class) {
      @Override
      public void internalHandle(Request request, RoutersClusterConfigResponse veniceResponse) {
        AdminSparkServer.validateParams(request, GET_ROUTERS_CLUSTER_CONFIG.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);
        veniceResponse.setCluster(clusterName);
        veniceResponse.setConfig(admin.getRoutersClusterConfig(clusterName));
      }
    };
  }
}
