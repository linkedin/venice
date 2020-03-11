package com.linkedin.venice.controller.server;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.MigrationPushStrategyResponse;
import java.util.Optional;
import org.apache.http.HttpStatus;
import spark.Route;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.*;
import static com.linkedin.venice.controllerapi.ControllerRoute.*;


public class MigrationRoutes extends AbstractRoute {
  public MigrationRoutes(Optional<DynamicAccessController> accessController) {
    super(accessController);
  }

  public Route getAllMigrationPushStrategies(Admin admin) {
    return (request, response) -> {
      MigrationPushStrategyResponse strategyResponse = new MigrationPushStrategyResponse();
      response.type(HttpConstants.JSON);
      try {
        // Only allow whitelist users to run this command
        if (!isWhitelistUsers(request)) {
          response.status(HttpStatus.SC_FORBIDDEN);
          strategyResponse.setError("Only admin users are allowed to run " + request.url());
          return AdminSparkServer.mapper.writeValueAsString(strategyResponse);
        }
        strategyResponse.setStrategies(admin.getAllStorePushStrategyForMigration());
      } catch (Throwable e) {
        strategyResponse.setError(e.getMessage());
      }
      return AdminSparkServer.mapper.writeValueAsString(strategyResponse);
    };
  }

  public Route setMigrationPushStrategy(Admin admin) {
    return (request, response) -> {
      ControllerResponse updateResponse = new ControllerResponse();
      response.type(HttpConstants.JSON);
      try {
        // Only allow whitelist users to run this command
        if (!isWhitelistUsers(request)) {
          response.status(HttpStatus.SC_FORBIDDEN);
          updateResponse.setError("Only admin users are allowed to run " + request.url());
          return AdminSparkServer.mapper.writeValueAsString(updateResponse);
        }
        AdminSparkServer.validateParams(request, SET_MIGRATION_PUSH_STRATEGY.getParams(), admin);
        String voldemortStoreName = request.queryParams(VOLDEMORT_STORE_NAME);
        String pushStrategy = request.queryParams(PUSH_STRATEGY);
        admin.setStorePushStrategyForMigration(voldemortStoreName, pushStrategy);
      } catch (Throwable e) {
        updateResponse.setError(e.getMessage());
      }
      response.type(HttpConstants.JSON);
      return AdminSparkServer.mapper.writeValueAsString(updateResponse);
    };
  }
}
