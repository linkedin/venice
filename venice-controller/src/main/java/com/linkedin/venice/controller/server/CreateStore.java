package com.linkedin.venice.controller.server;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.exceptions.VeniceHttpException;
import com.linkedin.venice.exceptions.VeniceStoreAlreadyExistsException;
import java.util.Optional;
import org.apache.http.HttpStatus;
import spark.Request;
import spark.Route;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.*;
import static com.linkedin.venice.controllerapi.ControllerRoute.NEW_STORE;


/**
 * Created by mwise on 5/18/16.
 */
public class CreateStore {
  public static final String DEFAULT_PRINCIPLES = "";

  public static Route getRoute(Admin admin) {
    return new VeniceRouteHandler<NewStoreResponse>(NewStoreResponse.class) {
      @Override
      public void internalHandle(Request request, NewStoreResponse veniceRepsonse) {
        AdminSparkServer.validateParams(request, NEW_STORE.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        String owner = request.queryParams(OWNER);
        String keySchema = request.queryParams(KEY_SCHEMA);
        String valueSchema = request.queryParams(VALUE_SCHEMA);

        // In order to keep the backward compatibility of this API, we treat principles as optional parameter.
        // If there is no value for principles, use the default one.
        String principles = AdminSparkServer.getOptionalParameterValue(request, PRINCIPLES, DEFAULT_PRINCIPLES);
        veniceRepsonse.setCluster(clusterName);
        veniceRepsonse.setName(storeName);
        veniceRepsonse.setOwner(owner);
        admin.addStore(clusterName, storeName, owner, principles, keySchema, valueSchema);
      }
    };
  }
}
