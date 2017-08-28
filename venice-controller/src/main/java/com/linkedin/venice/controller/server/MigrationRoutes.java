package com.linkedin.venice.controller.server;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.MigrationPushStrategyResponse;
import spark.Route;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.*;
import static com.linkedin.venice.controllerapi.ControllerRoute.*;


public class MigrationRoutes {
  private MigrationRoutes() {}

  public static Route getAllMigrationPushStrategies(Admin admin) {
    return (request, response) -> {
      MigrationPushStrategyResponse strategyResponse = new MigrationPushStrategyResponse();
      try {
        strategyResponse.setStrategies(admin.getAllStorePushStrategyForMigration());
      } catch (Throwable e) {
        strategyResponse.setError(e.getMessage());
      }
      response.type(HttpConstants.JSON);
      return AdminSparkServer.mapper.writeValueAsString(strategyResponse);
    };
  }

  public static Route setMigrationPushStrategy(Admin admin) {
    return (request, response) -> {
      ControllerResponse updateResponse = new ControllerResponse();
      try {
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
