package com.linkedin.venice.controller.server;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.MigrationPushStrategyResponse;
import com.linkedin.venice.exceptions.ErrorType;
import java.util.Optional;
import org.apache.http.HttpStatus;
import spark.Route;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.*;
import static com.linkedin.venice.controllerapi.ControllerRoute.*;


public class MigrationRoutes extends AbstractRoute {
  public MigrationRoutes(boolean sslEnabled, Optional<DynamicAccessController> accessController) {
    super(sslEnabled, accessController);
  }

  /**
   * No ACL check; any user is allowed to check migration strategy.
   */
  public Route getAllMigrationPushStrategies(Admin admin) {
    return (request, response) -> {
      MigrationPushStrategyResponse strategyResponse = new MigrationPushStrategyResponse();
      response.type(HttpConstants.JSON);
      try {
        strategyResponse.setStrategies(admin.getAllStorePushStrategyForMigration());
      } catch (Throwable e) {
        strategyResponse.setError(e);
      }
      return AdminSparkServer.mapper.writeValueAsString(strategyResponse);
    };
  }

  public Route setMigrationPushStrategy(Admin admin) {
    return (request, response) -> {
      ControllerResponse updateResponse = new ControllerResponse();
      response.type(HttpConstants.JSON);
      try {
        // Only allow allowlist users to run this command
        if (!isAllowListUser(request)) {
          response.status(HttpStatus.SC_FORBIDDEN);
          updateResponse.setError("Only admin users are allowed to run " + request.url());
          updateResponse.setErrorType(ErrorType.BAD_REQUEST);
          return AdminSparkServer.mapper.writeValueAsString(updateResponse);
        }
        AdminSparkServer.validateParams(request, SET_MIGRATION_PUSH_STRATEGY.getParams(), admin);
        String voldemortStoreName = request.queryParams(VOLDEMORT_STORE_NAME);
        String pushStrategy = request.queryParams(PUSH_STRATEGY);
        admin.setStorePushStrategyForMigration(voldemortStoreName, pushStrategy);
      } catch (Throwable e) {
        updateResponse.setError(e);
      }
      response.type(HttpConstants.JSON);
      return AdminSparkServer.mapper.writeValueAsString(updateResponse);
    };
  }
}
