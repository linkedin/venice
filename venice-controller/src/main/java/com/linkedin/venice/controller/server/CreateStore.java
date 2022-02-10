package com.linkedin.venice.controller.server;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.AclResponse;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import java.util.Optional;
import spark.Request;
import spark.Route;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.*;
import static com.linkedin.venice.controllerapi.ControllerRoute.*;


public class CreateStore extends AbstractRoute {
  public CreateStore(Optional<DynamicAccessController> accessController) {
    super(accessController);
  }

  public Route createStore(Admin admin) {
    return new VeniceRouteHandler<NewStoreResponse>(NewStoreResponse.class) {
      @Override
      public void internalHandle(Request request, NewStoreResponse veniceResponse) {
        // Only allow whitelist users to run this command
        if (!isAllowListUser(request)) {
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

        String accessPerm = request.queryParams(ACCESS_PERMISSION);
        Optional<String> accessPermissions = (accessPerm == null ? Optional.empty() : Optional.of(accessPerm));

        veniceResponse.setCluster(clusterName);
        veniceResponse.setName(storeName);
        veniceResponse.setOwner(owner);
        admin.createStore(clusterName, storeName, owner, keySchema, valueSchema, isSystemStore, accessPermissions);
      }
    };
  }

  public Route updateAclForStore(Admin admin) {
    return (request, response) -> {
      AclResponse responseObject = new AclResponse();
      response.type(HttpConstants.JSON);
      try {
        //TODO need security validation here?
        AdminSparkServer.validateParams(request, UPDATE_ACL.getParams(), admin);
        String cluster = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        String accessPermissions = request.queryParams(ACCESS_PERMISSION);
        responseObject.setCluster(cluster);
        responseObject.setName(storeName);
        admin.updateAclForStore(cluster, storeName, accessPermissions);
      } catch (Throwable e) {
        responseObject.setError(e.getMessage());
        AdminSparkServer.handleError(e, request, response);
      }
      return AdminSparkServer.mapper.writeValueAsString(responseObject);
    };
  }

  public Route getAclForStore(Admin admin) {
    return (request, response) -> {
      AclResponse responseObject = new AclResponse();
      response.type(HttpConstants.JSON);
      try {
        //TODO need security validation here?
        AdminSparkServer.validateParams(request, GET_ACL.getParams(), admin);
        String cluster = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        responseObject.setCluster(cluster);
        responseObject.setName(storeName);

        String accessPerm = admin.getAclForStore(cluster, storeName);
        responseObject.setAccessPermissions(accessPerm);
      } catch (Throwable e) {
        responseObject.setError(e.getMessage());
        AdminSparkServer.handleError(e, request, response);
      }
      return AdminSparkServer.mapper.writeValueAsString(responseObject);
    };
  }

  public Route deleteAclForStore(Admin admin) {
    return (request, response) -> {
      AclResponse responseObject = new AclResponse();
      response.type(HttpConstants.JSON);
      try {
        //TODO need security validation here?
        AdminSparkServer.validateParams(request, DELETE_ACL.getParams(), admin);
        String cluster = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        responseObject.setCluster(cluster);
        responseObject.setName(storeName);
        admin.deleteAclForStore(cluster, storeName);
      } catch (Throwable e) {
        responseObject.setError(e.getMessage());
        AdminSparkServer.handleError(e, request, response);
      }
      return AdminSparkServer.mapper.writeValueAsString(responseObject);
    };
  }

  public Route checkResourceCleanupForStoreCreation(Admin admin) {
    return (request, response) -> {
      ControllerResponse controllerResponse = new ControllerResponse();
      response.type(HttpConstants.JSON);
      try {
        AdminSparkServer.validateParams(request, GET_ACL.getParams(), admin);
        String cluster = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        controllerResponse.setCluster(cluster);
        controllerResponse.setName(storeName);
        admin.checkResourceCleanupBeforeStoreCreation(cluster, storeName);
      } catch (Throwable e) {
        controllerResponse.setError(e.getMessage());
        AdminSparkServer.handleError(e, request, response);
      }
      return AdminSparkServer.mapper.writeValueAsString(controllerResponse);
    };
  }

}
