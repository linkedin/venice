package com.linkedin.venice.controller.server;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.PUSH_STRATEGY;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.VOLDEMORT_STORE_NAME;
import static com.linkedin.venice.controllerapi.ControllerRoute.SET_MIGRATION_PUSH_STRATEGY;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.MigrationPushStrategyResponse;
import com.linkedin.venice.exceptions.ErrorType;
import java.util.Optional;
import org.apache.http.HttpStatus;
import spark.Route;


public class MigrationRoutes extends AbstractRoute {
  public MigrationRoutes(
      boolean sslEnabled,
      Optional<DynamicAccessController> accessController,
      VeniceControllerRequestHandler requestHandler) {
    super(sslEnabled, accessController, requestHandler);
  }

  /**
   * No ACL check; any user is allowed to check migration strategy.
   * @see Admin#getAllStorePushStrategyForMigration()
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
      return AdminSparkServer.OBJECT_MAPPER.writeValueAsString(strategyResponse);
    };
  }

  /**
   * @see Admin#setStorePushStrategyForMigration(String, String)
   */
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
          return AdminSparkServer.OBJECT_MAPPER.writeValueAsString(updateResponse);
        }
        AdminSparkServer.validateParams(request, SET_MIGRATION_PUSH_STRATEGY.getParams(), admin);
        String voldemortStoreName = request.queryParams(VOLDEMORT_STORE_NAME);
        String pushStrategy = request.queryParams(PUSH_STRATEGY);
        admin.setStorePushStrategyForMigration(voldemortStoreName, pushStrategy);
      } catch (Throwable e) {
        updateResponse.setError(e);
      }
      response.type(HttpConstants.JSON);
      return AdminSparkServer.OBJECT_MAPPER.writeValueAsString(updateResponse);
    };
  }
}
