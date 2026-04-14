package com.linkedin.venice.controller.server;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.CLUSTER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.DATACENTER_NAME;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.FABRIC;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.NAME;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.OPERATOR_ID;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.TIMEOUT_MINUTES;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.VERSION;
import static com.linkedin.venice.controllerapi.ControllerRoute.CLEANUP_INSTANCE_CUSTOMIZED_STATES;
import static com.linkedin.venice.controllerapi.ControllerRoute.GET_DEGRADED_DCS;
import static com.linkedin.venice.controllerapi.ControllerRoute.MARK_DC_DEGRADED;
import static com.linkedin.venice.controllerapi.ControllerRoute.STORE_MIGRATION_ALLOWED;
import static com.linkedin.venice.controllerapi.ControllerRoute.UNMARK_DC_DEGRADED;
import static com.linkedin.venice.controllerapi.ControllerRoute.UPDATE_CLUSTER_CONFIG;
import static com.linkedin.venice.controllerapi.ControllerRoute.UPDATE_DARK_CLUSTER_CONFIG;
import static com.linkedin.venice.controllerapi.ControllerRoute.WIPE_CLUSTER;

import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.DegradedDcResponse;
import com.linkedin.venice.controllerapi.MultiStoreTopicsResponse;
import com.linkedin.venice.controllerapi.StoreMigrationResponse;
import com.linkedin.venice.controllerapi.UpdateClusterConfigQueryParams;
import com.linkedin.venice.controllerapi.UpdateDarkClusterConfigQueryParams;
import com.linkedin.venice.protocols.controller.StoreMigrationCheckGrpcRequest;
import com.linkedin.venice.protocols.controller.StoreMigrationCheckGrpcResponse;
import com.linkedin.venice.utils.Utils;
import java.util.Map;
import java.util.Optional;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import spark.Request;
import spark.Route;


public class ClusterRoutes extends AbstractRoute {
  private static final Logger LOGGER = LogManager.getLogger(ClusterRoutes.class);
  // Default timeout for degraded DC mark. Advisory only — used for alerting, not auto-unmark.
  // 120 minutes allows a typical batch push cycle (~60 min) plus investigation buffer.
  private static final int DEFAULT_DEGRADED_TIMEOUT_MINUTES = 120;
  private final ClusterAdminOpsRequestHandler clusterAdminOpsRequestHandler;

  public ClusterRoutes(boolean sslEnabled, Optional<DynamicAccessController> accessController) {
    this(sslEnabled, accessController, null);
  }

  public ClusterRoutes(
      boolean sslEnabled,
      Optional<DynamicAccessController> accessController,
      ClusterAdminOpsRequestHandler clusterAdminOpsRequestHandler) {
    super(sslEnabled, accessController);
    this.clusterAdminOpsRequestHandler = clusterAdminOpsRequestHandler;
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
   * @see Admin#updateDarkClusterConfig(String, UpdateDarkClusterConfigQueryParams)
   */
  public Route updateDarkClusterConfig(Admin admin) {
    return new VeniceRouteHandler<ControllerResponse>(ControllerResponse.class) {
      @Override
      public void internalHandle(Request request, ControllerResponse veniceResponse) {
        AdminSparkServer.validateParams(request, UPDATE_DARK_CLUSTER_CONFIG.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);
        veniceResponse.setCluster(clusterName);
        Map<String, String> params = Utils.extractQueryParamsFromRequest(request.queryMap().toMap(), veniceResponse);
        try {
          admin.updateDarkClusterConfig(clusterName, new UpdateDarkClusterConfigQueryParams(params));
        } catch (Exception e) {
          veniceResponse.setError(
              "Failed when updating dark configs for cluster: " + clusterName + ". Exception type: " + e.getClass()
                  + ". Detailed message = " + e.getMessage());
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

        StoreMigrationCheckGrpcRequest grpcRequest =
            StoreMigrationCheckGrpcRequest.newBuilder().setClusterName(clusterName).build();

        StoreMigrationCheckGrpcResponse grpcResponse =
            clusterAdminOpsRequestHandler.isStoreMigrationAllowed(grpcRequest);

        veniceResponse.setStoreMigrationAllowed(grpcResponse.getStoreMigrationAllowed());
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

  public Route markDatacenterDegraded(Admin admin) {
    return new VeniceRouteHandler<ControllerResponse>(ControllerResponse.class) {
      @Override
      public void internalHandle(Request request, ControllerResponse veniceResponse) {
        if (!admin.isParent()) {
          veniceResponse.setError("Degraded mode operations are only supported on the parent controller.");
          return;
        }
        if (!checkIsAllowListUser(request, veniceResponse, () -> isAllowListUser(request))) {
          return;
        }
        AdminSparkServer.validateParams(request, MARK_DC_DEGRADED.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);
        String datacenterName = request.queryParams(DATACENTER_NAME);
        String timeoutStr = request.queryParams(TIMEOUT_MINUTES);
        int timeoutMinutes = DEFAULT_DEGRADED_TIMEOUT_MINUTES;
        if (timeoutStr != null) {
          try {
            timeoutMinutes = Integer.parseInt(timeoutStr);
          } catch (NumberFormatException e) {
            veniceResponse.setError("Invalid timeout_minutes value: " + timeoutStr);
            return;
          }
        }
        // operatorId is optional to support scripted/automated tooling that may not have a user
        // context. We log a warning rather than rejecting so that emergency degradation is never
        // blocked by a missing parameter. The ACL gate already authenticates the caller.
        String operatorId = request.queryParams(OPERATOR_ID);
        if (operatorId == null || operatorId.isEmpty()) {
          LOGGER.warn(
              "markDatacenterDegraded called without operatorId for DC: {} in cluster: {}. "
                  + "Set operator_id for audit trail.",
              datacenterName,
              clusterName);
          operatorId = "unknown";
        }
        veniceResponse.setCluster(clusterName);
        admin.markDatacenterDegraded(clusterName, datacenterName, timeoutMinutes, operatorId);
      }
    };
  }

  public Route unmarkDatacenterDegraded(Admin admin) {
    return new VeniceRouteHandler<ControllerResponse>(ControllerResponse.class) {
      @Override
      public void internalHandle(Request request, ControllerResponse veniceResponse) {
        if (!admin.isParent()) {
          veniceResponse.setError("Degraded mode operations are only supported on the parent controller.");
          return;
        }
        if (!checkIsAllowListUser(request, veniceResponse, () -> isAllowListUser(request))) {
          return;
        }
        AdminSparkServer.validateParams(request, UNMARK_DC_DEGRADED.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);
        String datacenterName = request.queryParams(DATACENTER_NAME);
        veniceResponse.setCluster(clusterName);
        admin.unmarkDatacenterDegraded(clusterName, datacenterName);
      }
    };
  }

  /**
   * No ACL check — any user is allowed to read the current degraded DC state.
   * This is intentional: read-only status queries should be accessible for monitoring and tooling.
   * Only mark/unmark operations require allowlist access.
   */
  public Route getDegradedDatacenters(Admin admin) {
    return new VeniceRouteHandler<DegradedDcResponse>(DegradedDcResponse.class) {
      @Override
      public void internalHandle(Request request, DegradedDcResponse veniceResponse) {
        if (!admin.isParent()) {
          veniceResponse.setError("Degraded mode operations are only supported on the parent controller.");
          return;
        }
        AdminSparkServer.validateParams(request, GET_DEGRADED_DCS.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);
        veniceResponse.setCluster(clusterName);
        veniceResponse.setDegradedDatacenters(admin.getDegradedDcStates(clusterName).getDegradedDatacenters());
      }
    };
  }
}
