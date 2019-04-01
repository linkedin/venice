package com.linkedin.venice.controller.server;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.utils.Utils;
import spark.Route;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.*;
import static com.linkedin.venice.controllerapi.ControllerRoute.*;

public class SkipAdminRoute {
  public static Route getRoute(Admin admin) {
    return (request, response) -> {
      ControllerResponse responseObject = new ControllerResponse();
      try {
        AdminSparkServer.validateParams(request, SKIP_ADMIN.getParams(), admin);
        responseObject.setCluster(request.queryParams(CLUSTER));
        long offset = Utils.parseLongFromString(request.queryParams(OFFSET), OFFSET);
        boolean skipDIV = Utils.parseBooleanFromString(request.queryParams(SKIP_DIV), SKIP_DIV);
        admin.skipAdminMessage(responseObject.getCluster(), offset, skipDIV);
      } catch (Throwable e) {
        responseObject.setError(e.getMessage());
        AdminSparkServer.handleError(e, request, response);
      }
      response.type(HttpConstants.JSON);
      return AdminSparkServer.mapper.writeValueAsString(responseObject);
    };
  }
}