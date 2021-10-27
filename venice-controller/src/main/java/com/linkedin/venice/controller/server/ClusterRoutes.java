package com.linkedin.venice.controller.server;

import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.StoreMigrationResponse;
import com.linkedin.venice.controllerapi.UpdateClusterConfigQueryParams;
import com.linkedin.venice.exceptions.VeniceException;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import spark.Request;
import spark.Route;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.*;
import static com.linkedin.venice.controllerapi.ControllerRoute.*;


public class ClusterRoutes  extends AbstractRoute {
  public ClusterRoutes(Optional<DynamicAccessController> accessController) {
    super(accessController);
  }

  public Route updateClusterConfig(Admin admin) {
    return new VeniceRouteHandler<ControllerResponse>(ControllerResponse.class) {
      @Override
      public void internalHandle(Request request, ControllerResponse veniceResponse) {
        // Only allow whitelist users to run this command
        if (!isWhitelistUsers(request)) {
          veniceResponse.setError("Only admin users are allowed to run " + request.url());
          return;
        }
        AdminSparkServer.validateParams(request, UPDATE_CLUSTER_CONFIG.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);

        veniceResponse.setCluster(clusterName);

        Map<String, String[]> sparkRequestParams = request.queryMap().toMap();

        boolean anyParamContainsMoreThanOneValue = sparkRequestParams.values().stream()
            .anyMatch(strings -> strings.length > 1);

        if (anyParamContainsMoreThanOneValue) {
          String errMsg =
              "Array parameters are not supported. Provided request parameters: " + sparkRequestParams.toString();
          veniceResponse.setError(errMsg);
          throw new VeniceException(errMsg);
        }

        Map<String, String> params = sparkRequestParams.entrySet().stream()
            // Extract the first (and only) value of each param
            .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()[0]));

        try {
          admin.updateClusterConfig(clusterName, new UpdateClusterConfigQueryParams(params));
        } catch (Exception e) {
          veniceResponse.setError("Failed when updating configs for cluster: " + clusterName + ". Exception type: " + e.getClass().toString() + ". Detailed message = " + e.getMessage());
        }
      }
    };
  }

  public Route isStoreMigrationAllowed(Admin admin) {
    return new VeniceRouteHandler<StoreMigrationResponse>(StoreMigrationResponse.class) {
      @Override
      public void internalHandle(Request request, StoreMigrationResponse veniceResponse) {
        // Only allow whitelist users to run this command
        if (!isWhitelistUsers(request)) {
          veniceResponse.setError("Only admin users are allowed to run " + request.url());
          return;
        }
        AdminSparkServer.validateParams(request, STORE_MIGRATION_ALLOWED.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);
        veniceResponse.setCluster(clusterName);
        veniceResponse.setStoreMigrationAllowed(admin.isStoreMigrationAllowed(clusterName));
      }
    };
  }
}
