package com.linkedin.venice.controller.server;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.MasterControllerResponse;
import com.linkedin.venice.exceptions.VeniceException;
import spark.Route;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.CLUSTER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.GET_MASTER_CONTROLLER_PARAMS;


public class MasterController {
  public static Route getRoute(Admin admin) {
    return (request, response) -> {
      MasterControllerResponse responseObject = new MasterControllerResponse();
      try {
        AdminSparkServer.validateParams(request, GET_MASTER_CONTROLLER_PARAMS, admin);
        String cluster = request.queryParams(CLUSTER);

        responseObject.setCluster(cluster);
        responseObject.setUrl(admin.getMasterController(cluster).getUrl());
      } catch (VeniceException e) {
        responseObject.setError(e.getMessage());
        AdminSparkServer.handleError(e, request, response);
      }
      response.type(HttpConstants.JSON);
      return AdminSparkServer.mapper.writeValueAsString(responseObject);
    };
  }
}
