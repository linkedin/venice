package com.linkedin.venice.controller.server;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import spark.Route;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.CLUSTER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.KEY_SCHEMA;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.NAME;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.OWNER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.VALUE_SCHEMA;
import static com.linkedin.venice.controllerapi.ControllerRoute.NEW_STORE;


/**
 * Created by mwise on 5/18/16.
 */
public class CreateStore {
  public static Route getRoute(Admin admin) {
    return (request, response) -> {
      NewStoreResponse responseObject = new NewStoreResponse();
      try {
        AdminSparkServer.validateParams(request, NEW_STORE.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        String owner = request.queryParams(OWNER);
        String keySchema = request.queryParams(KEY_SCHEMA);
        String valueSchema = request.queryParams(VALUE_SCHEMA);

        responseObject.setCluster(clusterName);
        responseObject.setName(storeName);
        responseObject.setOwner(owner);
        admin.addStore(clusterName, storeName, owner, keySchema, valueSchema);
      } catch (Throwable e) {
        responseObject.setError(e.getMessage());
        AdminSparkServer.handleError(e, request, response);
      }
      response.type(HttpConstants.JSON);
      return AdminSparkServer.mapper.writeValueAsString(responseObject);
    };
  }
}
