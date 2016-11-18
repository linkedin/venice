package com.linkedin.venice.controller.server;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.MasterControllerResponse;
import spark.Route;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.CLUSTER;
import static com.linkedin.venice.controllerapi.ControllerRoute.MASTER_CONTROLLER;


public class MasterController {
  public static Route getRoute(Admin admin) {
    return (request, response) -> {
      MasterControllerResponse responseObject = new MasterControllerResponse();
      try {
        AdminSparkServer.validateParams(request, MASTER_CONTROLLER.getParams(), admin);
        String cluster = request.queryParams(CLUSTER);

        responseObject.setCluster(cluster);
        responseObject.setUrl(admin.getMasterController(cluster).getUrl());
      } catch (Throwable e) {
        responseObject.setError(e.getMessage());
        AdminSparkServer.handleError(e, request, response);
      }
      response.type(HttpConstants.JSON);
      return AdminSparkServer.mapper.writeValueAsString(responseObject);
    };
  }
}
