package com.linkedin.venice.controller.server;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.exceptions.VeniceException;
import spark.Route;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.CLUSTER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.NAME;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.NEWSTORE_PARAMS;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.OWNER;


/**
 * Created by mwise on 5/18/16.
 */
public class CreateStore {
  public static Route getRoute(Admin admin) {
    return (request, response) -> {
      NewStoreResponse responseObject = new NewStoreResponse();
      try {
        AdminSparkServer.validateParams(request, NEWSTORE_PARAMS, admin);
        responseObject.setCluster(request.queryParams(CLUSTER));
        responseObject.setName(request.queryParams(NAME));
        responseObject.setOwner(request.queryParams(OWNER));
        admin.addStore(responseObject.getCluster(), responseObject.getName(), responseObject.getOwner());
      } catch (VeniceException e) {
        responseObject.setError(e.getMessage());
        AdminSparkServer.handleError(e, request, response);
      }
      response.type(HttpConstants.JSON);
      return AdminSparkServer.mapper.writeValueAsString(responseObject);
    };
  }
}
