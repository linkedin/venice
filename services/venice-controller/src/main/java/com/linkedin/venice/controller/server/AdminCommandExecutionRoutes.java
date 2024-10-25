package com.linkedin.venice.controller.server;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.CLUSTER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.EXECUTION_ID;
import static com.linkedin.venice.controllerapi.ControllerRoute.EXECUTION;
import static com.linkedin.venice.controllerapi.ControllerRoute.LAST_SUCCEED_EXECUTION_ID;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.LastSucceedExecutionIdResponse;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.request.AdminCommandExecutionStatusRequest;
import com.linkedin.venice.controllerapi.request.ControllerRequest;
import com.linkedin.venice.controllerapi.routes.AdminCommandExecutionResponse;
import java.util.Optional;
import spark.Route;


public class AdminCommandExecutionRoutes extends AbstractRoute {
  public AdminCommandExecutionRoutes(
      boolean sslEnabled,
      Optional<DynamicAccessController> accessController,
      VeniceControllerRequestHandler requestHandler) {
    super(sslEnabled, accessController, requestHandler);
  }

  /**
   * No ACL check; any user is allowed to check admin command execution status.
   * @see Admin#getAdminCommandExecutionTracker(String)
   */
  public Route getExecution(Admin admin) {
    return (request, response) -> {
      AdminCommandExecutionResponse responseObject = new AdminCommandExecutionResponse();
      response.type(HttpConstants.JSON);
      // This request should only hit the parent controller. If a PROD controller get this kind of request, a empty
      // response would be return.
      AdminSparkServer.validateParams(request, EXECUTION.getParams(), admin);
      try {
        requestHandler.getAdminCommandExecutionStatus(
            new AdminCommandExecutionStatusRequest(
                request.queryParams(CLUSTER),
                Long.parseLong(request.queryParams(EXECUTION_ID))),
            responseObject);
      } catch (Throwable e) {
        responseObject.setError(e.getMessage());
        AdminSparkServer.handleError(e, request, response);
      }
      return AdminSparkServer.OBJECT_MAPPER.writeValueAsString(responseObject);
    };
  }

  /**
   * No ACL check; any user is allowed to check last succeeded execution Id.
   * @see Admin#getLastSucceedExecutionId(String)
   */
  public Route getLastSucceedExecutionId(Admin admin) {
    return (request, response) -> {
      LastSucceedExecutionIdResponse responseObject = new LastSucceedExecutionIdResponse();
      response.type(HttpConstants.JSON);
      AdminSparkServer.validateParams(request, LAST_SUCCEED_EXECUTION_ID.getParams(), admin);
      try {
        requestHandler.getLastSucceedExecutionId(new ControllerRequest(request.queryParams(CLUSTER)), responseObject);
      } catch (Throwable e) {
        responseObject.setError(e);
        AdminSparkServer.handleError(e, request, response);
      }
      return AdminSparkServer.OBJECT_MAPPER.writeValueAsString(responseObject);
    };
  }
}
