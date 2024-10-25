package com.linkedin.venice.controller.server;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.CLUSTER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.OFFSET;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.SKIP_DIV;
import static com.linkedin.venice.controllerapi.ControllerRoute.SKIP_ADMIN;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.exceptions.ErrorType;
import com.linkedin.venice.utils.Utils;
import java.util.Optional;
import org.apache.http.HttpStatus;
import spark.Route;


public class SkipAdminRoute extends AbstractRoute {
  public SkipAdminRoute(
      boolean sslEnabled,
      Optional<DynamicAccessController> accessController,
      VeniceControllerRequestHandler requestHandler) {
    super(sslEnabled, accessController, requestHandler);
  }

  /**
   * @see Admin#skipAdminMessage(String, long, boolean)
   */
  public Route skipAdminMessage(Admin admin) {
    return (request, response) -> {
      ControllerResponse responseObject = new ControllerResponse();
      response.type(HttpConstants.JSON);
      try {
        // Only allow allowlist users to run this command
        if (!isAllowListUser(request)) {
          response.status(HttpStatus.SC_FORBIDDEN);
          responseObject.setError("Only admin users are allowed to run " + request.url());
          responseObject.setErrorType(ErrorType.BAD_REQUEST);
          return AdminSparkServer.OBJECT_MAPPER.writeValueAsString(responseObject);
        }
        AdminSparkServer.validateParams(request, SKIP_ADMIN.getParams(), admin);
        responseObject.setCluster(request.queryParams(CLUSTER));
        long offset = Utils.parseLongFromString(request.queryParams(OFFSET), OFFSET);
        boolean skipDIV = Utils.parseBooleanFromString(request.queryParams(SKIP_DIV), SKIP_DIV);
        admin.skipAdminMessage(responseObject.getCluster(), offset, skipDIV);
      } catch (Throwable e) {
        responseObject.setError(e);
        AdminSparkServer.handleError(e, request, response);
      }
      return AdminSparkServer.OBJECT_MAPPER.writeValueAsString(responseObject);
    };
  }
}
