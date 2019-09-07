package com.linkedin.venice.controller.server;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.LastSucceedExecutionIdResponse;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.AdminCommandExecutionTracker;
import com.linkedin.venice.controllerapi.AdminCommandExecution;
import com.linkedin.venice.controllerapi.routes.AdminCommandExecutionResponse;
import java.util.Optional;
import spark.Route;

import static com.linkedin.venice.controllerapi.ControllerRoute.EXECUTION;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.*;
import static com.linkedin.venice.controllerapi.ControllerRoute.LAST_SUCCEED_EXECUTION_ID;


public class AdminCommandExecutionRoutes extends AbstractRoute {
  public AdminCommandExecutionRoutes(Optional<DynamicAccessController> accessController) {
    super(accessController);
  }

  public Route getExecution(Admin admin) {
    return (request, response) -> {
      // TODO: Only allow whitelist users to run this command
      // This request should only hit the parent controller. If a PROD controller get this kind of request, a empty
      // response would be return.
      AdminCommandExecutionResponse responseObject = new AdminCommandExecutionResponse();
      AdminSparkServer.validateParams(request, EXECUTION.getParams(), admin);
      String cluster = request.queryParams(CLUSTER);
      long executionId = Long.valueOf(request.queryParams(EXECUTION_ID));
      responseObject.setCluster(cluster);
      Optional<AdminCommandExecutionTracker> adminCommandExecutionTracker = admin.getAdminCommandExecutionTracker(cluster);
      if (adminCommandExecutionTracker.isPresent()) {
        AdminCommandExecution execution = adminCommandExecutionTracker.get().checkExecutionStatus(executionId);
        if (execution == null) {
          responseObject.setError(
              "Could not find the execution by given id: " + executionId + " in cluster: " + cluster);
        } else {
          responseObject.setExecution(execution);
        }
      } else {
        responseObject.setError(
            "Could not track execution in this controller. Make sure you send the command to a correct parent controller.");
      }
      response.type(HttpConstants.JSON);
      return AdminSparkServer.mapper.writeValueAsString(responseObject);
    };
  }

  public Route getLastSucceedExecutionId(Admin admin) {
    return (request, response) -> {
      // TODO: Only allow whitelist users to run this command
      LastSucceedExecutionIdResponse reponseObject = new LastSucceedExecutionIdResponse();
      AdminSparkServer.validateParams(request, LAST_SUCCEED_EXECUTION_ID.getParams(), admin);
      String cluster = request.queryParams(CLUSTER);
      reponseObject.setCluster(cluster);
      try {
        reponseObject.setLastSucceedExecutionId(admin.getLastSucceedExecutionId(cluster));
      } catch (Throwable e) {
        reponseObject.setError(e.getMessage());
        AdminSparkServer.handleError(e, request, response);
      }
      response.type(HttpConstants.JSON);
      return AdminSparkServer.mapper.writeValueAsString(reponseObject);
    };
  }
}
