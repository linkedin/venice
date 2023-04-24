package com.linkedin.venice.controller.server;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.CLUSTER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.FABRIC;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.NAME;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.VERSION;
import static com.linkedin.venice.controllerapi.ControllerRoute.CLEANUP_INSTANCE_CUSTOMIZED_STATES;
import static com.linkedin.venice.controllerapi.ControllerRoute.STORE_MIGRATION_ALLOWED;
import static com.linkedin.venice.controllerapi.ControllerRoute.UPDATE_CLUSTER_CONFIG;
import static com.linkedin.venice.controllerapi.ControllerRoute.WIPE_CLUSTER;

import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.authentication.AuthenticationService;
import com.linkedin.venice.authorization.AuthorizerService;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.MultiStoreTopicsResponse;
import com.linkedin.venice.controllerapi.StoreMigrationResponse;
import com.linkedin.venice.controllerapi.UpdateClusterConfigQueryParams;
import com.linkedin.venice.utils.Utils;
import java.util.Map;
import java.util.Optional;
import spark.Request;
import spark.Route;


public class ClusterRoutes extends AbstractRoute {
  public ClusterRoutes(
      boolean sslEnabled,
      Optional<DynamicAccessController> accessController,
      Optional<AuthenticationService> authenticationService,
      Optional<AuthorizerService> authorizerService) {
    super(sslEnabled, accessController, authenticationService, authorizerService);
  }

  /**
   * @see Admin#updateClusterConfig(String, UpdateClusterConfigQueryParams)
   */
  public Route updateClusterConfig(Admin admin) {
    return new VeniceRouteHandler<ControllerResponse>(ControllerResponse.class) {
      @Override
      public void internalHandle(Request request, ControllerResponse veniceResponse) {
        // Only allow allowlist users to run this command
        if (!checkIsAllowListUser(request, veniceResponse, () -> isAllowListUser(request))) {
          return;
        }
        AdminSparkServer.validateParams(request, UPDATE_CLUSTER_CONFIG.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);

        veniceResponse.setCluster(clusterName);

        Map<String, String> params = Utils.extractQueryParamsFromRequest(request.queryMap().toMap(), veniceResponse);

        try {
          admin.updateClusterConfig(clusterName, new UpdateClusterConfigQueryParams(params));
        } catch (Exception e) {
          veniceResponse.setError(
              "Failed when updating configs for cluster: " + clusterName + ". Exception type: "
                  + e.getClass().toString() + ". Detailed message = " + e.getMessage());
        }
      }
    };
  }

  /**
   * No ACL check; any user is allowed to check whether store migration is allowed for a specific cluster.
   * @see Admin#isStoreMigrationAllowed(String)
   */
  public Route isStoreMigrationAllowed(Admin admin) {
    return new VeniceRouteHandler<StoreMigrationResponse>(StoreMigrationResponse.class) {
      @Override
      public void internalHandle(Request request, StoreMigrationResponse veniceResponse) {
        AdminSparkServer.validateParams(request, STORE_MIGRATION_ALLOWED.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);
        veniceResponse.setCluster(clusterName);
        veniceResponse.setStoreMigrationAllowed(admin.isStoreMigrationAllowed(clusterName));
      }
    };
  }

  /**
   * @see Admin#wipeCluster(String, String, Optional, Optional)
   */
  public Route wipeCluster(Admin admin) {
    return new VeniceRouteHandler<ControllerResponse>(ControllerResponse.class) {
      @Override
      public void internalHandle(Request request, ControllerResponse veniceResponse) {
        // Only allow allowlist users to run this command
        if (!checkIsAllowListUser(request, veniceResponse, () -> isAllowListUser(request))) {
          return;
        }
        AdminSparkServer.validateParams(request, WIPE_CLUSTER.getParams(), admin);
        String cluster = request.queryParams(CLUSTER);
        String fabric = request.queryParams(FABRIC);
        Optional<String> storeName = Optional.ofNullable(request.queryParams(NAME));
        Optional<Integer> versionNum = Optional.ofNullable(request.queryParams(VERSION)).map(Integer::parseInt);
        veniceResponse.setCluster(cluster);
        storeName.ifPresent(veniceResponse::setName);
        admin.wipeCluster(cluster, fabric, storeName, versionNum);
      }
    };
  }

  /**
   * Endpoint intended to be called via the admin tool manually to trigger cleanup for any lingering ZNodes produced
   * from bugs/errors for instance level customized states.
   * @see Admin#cleanupInstanceCustomizedStates(String)
   */
  public Route cleanupInstanceCustomizedStates(Admin admin) {
    return new VeniceRouteHandler<MultiStoreTopicsResponse>(MultiStoreTopicsResponse.class) {
      @Override
      public void internalHandle(Request request, MultiStoreTopicsResponse veniceResponse) {
        AdminSparkServer.validateParams(request, CLEANUP_INSTANCE_CUSTOMIZED_STATES.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);
        veniceResponse.setCluster(clusterName);
        veniceResponse.setTopics(admin.cleanupInstanceCustomizedStates(clusterName));
      }
    };
  }
}
