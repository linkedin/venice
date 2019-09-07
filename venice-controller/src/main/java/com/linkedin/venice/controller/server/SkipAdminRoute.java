package com.linkedin.venice.controller.server;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.utils.Utils;
import java.util.Optional;
import spark.Route;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.*;
import static com.linkedin.venice.controllerapi.ControllerRoute.*;

public class SkipAdminRoute extends AbstractRoute {
  public SkipAdminRoute(Optional<DynamicAccessController> accessController) {
    super(accessController);
  }

  public Route getRoute(Admin admin) {
    return (request, response) -> {
      ControllerResponse responseObject = new ControllerResponse();
      try {
        // TODO: Only allow whitelist users to run this command
        AdminSparkServer.validateParams(request, SKIP_ADMIN.getParams(), admin);
        responseObject.setCluster(request.queryParams(CLUSTER));
        long offset = Utils.parseLongFromString(request.queryParams(OFFSET), OFFSET);
        boolean skipDIV = Utils.parseBooleanFromString(request.queryParams(SKIP_DIV), SKIP_DIV);
        admin.skipAdminMessage(responseObject.getCluster(), offset, skipDIV);
      } catch (Throwable e) {
        responseObject.setError(e.getMessage());
        AdminSparkServer.handleError(e, request, response);
      }
      response.type(HttpConstants.JSON);
      return AdminSparkServer.mapper.writeValueAsString(responseObject);
    };
  }
}