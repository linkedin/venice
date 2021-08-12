package com.linkedin.venice.controller.server;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.ChildAwareResponse;
import com.linkedin.venice.controllerapi.MasterControllerResponse;
import java.util.Map;
import java.util.Optional;
import spark.Request;
import spark.Route;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.CLUSTER;
import static com.linkedin.venice.controllerapi.ControllerRoute.*;


public class ControllerRoutes extends AbstractRoute {

  public ControllerRoutes(Optional<DynamicAccessController> accessController) {
    super(accessController);
  }

  /**
   * No ACL check; any user is allowed to check master controller.
   */
  public Route getMasterController(Admin admin) {
    return (request, response) -> {
      MasterControllerResponse responseObject = new MasterControllerResponse();
      try {
        AdminSparkServer.validateParams(request, MASTER_CONTROLLER.getParams(), admin);
        String cluster = request.queryParams(CLUSTER);
        responseObject.setCluster(cluster);
        responseObject.setUrl(admin.getLeaderController(cluster).getUrl(isAclEnabled()));
      } catch (Throwable e) {
        responseObject.setError(e.getMessage());
        AdminSparkServer.handleError(e, request, response);
      }
      response.type(HttpConstants.JSON);
      return AdminSparkServer.mapper.writeValueAsString(responseObject);
    };
  }

  public Route getChildControllers(Admin admin) {
    return new VeniceRouteHandler<ChildAwareResponse>(ChildAwareResponse.class) {
      @Override
      public void internalHandle(Request request, ChildAwareResponse veniceResponse) {
        AdminSparkServer.validateParams(request, LIST_CHILD_CLUSTERS.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);

        veniceResponse.setCluster(clusterName);

        if (admin.isParent()) {
          Map<String, String> childControllerUrls = admin.getChildDataCenterControllerUrlMap(clusterName);
          veniceResponse.setChildClusterMap(childControllerUrls);
        }
      }
    };
  }
}
