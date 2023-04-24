package com.linkedin.venice.controller.server;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.CLUSTER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.EXECUTION_ID;
import static com.linkedin.venice.controllerapi.ControllerRoute.EXECUTION;
import static com.linkedin.venice.controllerapi.ControllerRoute.LAST_SUCCEED_EXECUTION_ID;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.LastSucceedExecutionIdResponse;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.authentication.AuthenticationService;
import com.linkedin.venice.authorization.AuthorizerService;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.AdminCommandExecutionTracker;
import com.linkedin.venice.controllerapi.AdminCommandExecution;
import com.linkedin.venice.controllerapi.routes.AdminCommandExecutionResponse;
import java.util.Optional;
import spark.Route;


public class AdminCommandExecutionRoutes extends AbstractRoute {
  public AdminCommandExecutionRoutes(
      boolean sslEnabled,
      Optional<DynamicAccessController> accessController,
      Optional<AuthenticationService> authenticationService,
      Optional<AuthorizerService> authorizerService) {
    super(sslEnabled, accessController, authenticationService, authorizerService);
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
      String cluster = request.queryParams(CLUSTER);
      long executionId = Long.parseLong(request.queryParams(EXECUTION_ID));
      responseObject.setCluster(cluster);
      Optional<AdminCommandExecutionTracker> adminCommandExecutionTracker =
          admin.getAdminCommandExecutionTracker(cluster);
      if (adminCommandExecutionTracker.isPresent()) {
        AdminCommandExecution execution = adminCommandExecutionTracker.get().checkExecutionStatus(executionId);
        if (execution == null) {
          responseObject
              .setError("Could not find the execution by given id: " + executionId + " in cluster: " + cluster);
        } else {
          responseObject.setExecution(execution);
        }
      } else {
        responseObject.setError(
            "Could not track execution in this controller. Make sure you send the command to a correct parent controller.");
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
      String cluster = request.queryParams(CLUSTER);
      responseObject.setCluster(cluster);
      try {
        responseObject.setLastSucceedExecutionId(admin.getLastSucceedExecutionId(cluster));
      } catch (Throwable e) {
        responseObject.setError(e);
        AdminSparkServer.handleError(e, request, response);
      }
      return AdminSparkServer.OBJECT_MAPPER.writeValueAsString(responseObject);
    };
  }
}
