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
      public void internalHandle(Request request, NewStoreResponse veniceRepsonse) {
        // TODO: Only allow whitelist users to run this command
        AdminSparkServer.validateParams(request, NEW_STORE.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        String keySchema = request.queryParams(KEY_SCHEMA);
        String valueSchema = request.queryParams(VALUE_SCHEMA);

        String owner = AdminSparkServer.getOptionalParameterValue(request, OWNER);
        if (owner == null) {
          owner = "";
        }

        veniceRepsonse.setCluster(clusterName);
        veniceRepsonse.setName(storeName);
        veniceRepsonse.setOwner(owner);
        admin.addStore(clusterName, storeName, owner, keySchema, valueSchema);
      }
    };
  }
}
