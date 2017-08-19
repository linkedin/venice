package com.linkedin.venice.controller.server;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.RoutersClusterConfigResponse;
import com.linkedin.venice.utils.Utils;
import java.util.Optional;
import spark.Request;
import spark.Route;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.*;
import static com.linkedin.venice.controllerapi.ControllerRoute.*;


public class RoutersClusterConfigRoutes {
  public static Route enableThrottling(Admin admin) {
    return new VeniceRouteHandler<ControllerResponse>(ControllerResponse.class) {
      @Override
      public void internalHandle(Request request, ControllerResponse veniceResponse) {
        AdminSparkServer.validateParams(request, ENABLE_THROTTLING.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);
        veniceResponse.setCluster(clusterName);
        boolean status = Utils.parseBooleanFromString(request.queryParams(STATUS), "enableThrottling");
        admin.updateRoutersClusterConfig(clusterName, Optional.of(status), Optional.empty(), Optional.empty(),
            Optional.empty());
      }
    };
  }

  public static Route enableMaxCapacityProtection(Admin admin) {
    return new VeniceRouteHandler<ControllerResponse>(ControllerResponse.class) {
      @Override
      public void internalHandle(Request request, ControllerResponse veniceResponse) {
        AdminSparkServer.validateParams(request, ENABLE_MAX_CAPACITY_PROTECTION.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);
        veniceResponse.setCluster(clusterName);
        boolean status = Utils.parseBooleanFromString(request.queryParams(STATUS), "enableMaxCapacityProtection");
        admin.updateRoutersClusterConfig(clusterName, Optional.empty(), Optional.empty(), Optional.of(status),
            Optional.empty());
      }
    };
  }

  public static Route enableQuotaRebalanced(Admin admin) {
    return new VeniceRouteHandler<ControllerResponse>(ControllerResponse.class) {
      @Override
      public void internalHandle(Request request, ControllerResponse veniceResponse) {
        AdminSparkServer.validateParams(request, ENABLE_QUOTA_REBALANCED.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);
        veniceResponse.setCluster(clusterName);
        boolean status = Utils.parseBooleanFromString(request.queryParams(STATUS), "enableQuotaRebalance");
        int expectedRouterCount =
            Utils.parseIntFromString(request.queryParams(EXPECTED_ROUTER_COUNT), "expectedRouterCount");
        admin.updateRoutersClusterConfig(clusterName, Optional.empty(), Optional.of(status), Optional.empty(),
            Optional.of(expectedRouterCount));
      }
    };
  }

  public static Route getRoutersClusterConfig(Admin admin) {
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
