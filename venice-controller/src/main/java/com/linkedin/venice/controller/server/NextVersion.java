package com.linkedin.venice.controller.server;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.VersionResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Version;
import spark.Route;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.CLUSTER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.NAME;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.NEXTVERSION_PARAMS;


/**
 * Created by mwise on 5/18/16.
 */
public class NextVersion {
  public static Route getRoute(Admin admin) {
    return (request, response) -> {
      VersionResponse responseObject = new VersionResponse();
      try {
        AdminSparkServer.validateParams(request, NEXTVERSION_PARAMS, admin);
        responseObject.setCluster(request.queryParams(CLUSTER));
        responseObject.setName(request.queryParams(NAME));
        Version version = admin.peekNextVersion(responseObject.getCluster(), responseObject.getName());
        responseObject.setVersion(version.getNumber());
      } catch (VeniceException e) {
        responseObject.setError(e.getMessage());
        AdminSparkServer.handleError(e, request, response);
      }
      response.type(HttpConstants.JSON);
      return AdminSparkServer.mapper.writeValueAsString(responseObject);
    };
  }
}
