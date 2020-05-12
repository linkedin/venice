package com.linkedin.venice.controller.server;

import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import java.util.Optional;
import spark.Request;
import spark.Route;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.*;
import static com.linkedin.venice.controllerapi.ControllerRoute.NEW_STORE;


public class CreateStore extends AbstractRoute {
  public CreateStore(Optional<DynamicAccessController> accessController) {
    super(accessController);
  }

  public Route addStore(Admin admin) {
    return new VeniceRouteHandler<NewStoreResponse>(NewStoreResponse.class) {
      @Override
      public void internalHandle(Request request, NewStoreResponse veniceResponse) {
        // Only allow whitelist users to run this command
        if (!isWhitelistUsers(request)) {
          veniceResponse.setError("Only admin users are allowed to run " + request.url());
          return;
        }
        AdminSparkServer.validateParams(request, NEW_STORE.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        String keySchema = request.queryParams(KEY_SCHEMA);
        String valueSchema = request.queryParams(VALUE_SCHEMA);
        boolean isSystemStore = Boolean.parseBoolean(request.queryParams(IS_SYSTEM_STORE));

        String owner = AdminSparkServer.getOptionalParameterValue(request, OWNER);
        if (owner == null) {
          owner = "";
        }

        veniceResponse.setCluster(clusterName);
        veniceResponse.setName(storeName);
        veniceResponse.setOwner(owner);
        admin.addStore(clusterName, storeName, owner, keySchema, valueSchema, isSystemStore);
      }
    };
  }
}
