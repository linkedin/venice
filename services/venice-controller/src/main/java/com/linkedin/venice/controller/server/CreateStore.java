package com.linkedin.venice.controller.server;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.*;
import static com.linkedin.venice.controllerapi.ControllerRoute.*;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.AclResponse;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import java.util.Optional;
import spark.Request;
import spark.Route;


public class CreateStore extends AbstractRoute {
  public CreateStore(boolean sslEnabled, Optional<DynamicAccessController> accessController) {
    super(sslEnabled, accessController);
  }

  /**
   * @see Admin#createStore(String, String, String, String, String, boolean, Optional)
   */
  public Route createStore(Admin admin) {
    return new VeniceRouteHandler<NewStoreResponse>(NewStoreResponse.class) {
      @Override
      public void internalHandle(Request request, NewStoreResponse veniceResponse) {
        // Only allow allowlist users to run this command
        if (!checkIsAllowListUser(request, veniceResponse, () -> isAllowListUser(request))) {
          return;
        }
        AdminSparkServer.validateParams(request, NEW_STORE.getParams(), admin);
        CreateStoreRequest createStoreRequest = new CreateStoreRequest();
        createStoreRequest.setClusterName(request.queryParams(CLUSTER));
        createStoreRequest.setStoreName(request.queryParams(NAME));
        createStoreRequest.setKeySchema(request.queryParams(KEY_SCHEMA));
        createStoreRequest.setValueSchema(request.queryParams(VALUE_SCHEMA));
        createStoreRequest.setSystemStore(Boolean.parseBoolean(request.queryParams(IS_SYSTEM_STORE)));
        createStoreRequest.setAccessPerm(request.queryParams(ACCESS_PERMISSION));

        String owner = AdminSparkServer.getOptionalParameterValue(request, OWNER);
        if (owner == null) {
          owner = "";
        }

        NewStoreResponse duplicateVeniceResponse = createStore(createStoreRequest, owner, admin);
        veniceResponse.setCluster(duplicateVeniceResponse.getCluster());
        veniceResponse.setName(duplicateVeniceResponse.getName());
        veniceResponse.setOwner(duplicateVeniceResponse.getOwner());
      }
    };
  }

  private NewStoreResponse createStore(CreateStoreRequest request, String owner, Admin admin) {
    NewStoreResponse response = new NewStoreResponse();
    response.setCluster(request.getClusterName());
    response.setName(request.getStoreName());
    response.setOwner(owner);
    admin.createStore(
        request.getClusterName(),
        request.getStoreName(),
        owner,
        request.getKeySchema(),
        request.getValueSchema(),
        request.isSystemStore(),
        Optional.ofNullable(request.getAccessPerm()));
    return response;
  }

  /**
   * @see Admin#updateAclForStore(String, String, String)
   */
  public Route updateAclForStore(Admin admin) {
    return (request, response) -> {
      AclResponse responseObject = new AclResponse();
      response.type(HttpConstants.JSON);
      try {
        // TODO need security validation here?
        AdminSparkServer.validateParams(request, UPDATE_ACL.getParams(), admin);
        String cluster = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        String accessPermissions = request.queryParams(ACCESS_PERMISSION);
        responseObject.setCluster(cluster);
        responseObject.setName(storeName);
        admin.updateAclForStore(cluster, storeName, accessPermissions);
      } catch (Throwable e) {
        responseObject.setError(e);
        AdminSparkServer.handleError(e, request, response);
      }
      return AdminSparkServer.OBJECT_MAPPER.writeValueAsString(responseObject);
    };
  }

  /**
   * @see Admin#getAclForStore(String, String)
   */
  public Route getAclForStore(Admin admin) {
    return (request, response) -> {
      AclResponse responseObject = new AclResponse();
      response.type(HttpConstants.JSON);
      try {
        // TODO need security validation here?
        AdminSparkServer.validateParams(request, GET_ACL.getParams(), admin);
        String cluster = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        responseObject.setCluster(cluster);
        responseObject.setName(storeName);

        String accessPerm = admin.getAclForStore(cluster, storeName);
        responseObject.setAccessPermissions(accessPerm);
      } catch (Throwable e) {
        responseObject.setError(e);
        AdminSparkServer.handleError(e, request, response);
      }
      return AdminSparkServer.OBJECT_MAPPER.writeValueAsString(responseObject);
    };
  }

  /**
   * @see Admin#deleteAclForStore(String, String)
   */
  public Route deleteAclForStore(Admin admin) {
    return (request, response) -> {
      AclResponse responseObject = new AclResponse();
      response.type(HttpConstants.JSON);
      try {
        // TODO need security validation here?
        AdminSparkServer.validateParams(request, DELETE_ACL.getParams(), admin);
        String cluster = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        responseObject.setCluster(cluster);
        responseObject.setName(storeName);
        admin.deleteAclForStore(cluster, storeName);
      } catch (Throwable e) {
        responseObject.setError(e);
        AdminSparkServer.handleError(e, request, response);
      }
      return AdminSparkServer.OBJECT_MAPPER.writeValueAsString(responseObject);
    };
  }

  /**
   * @see Admin#checkResourceCleanupBeforeStoreCreation(String, String)
   */
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
        controllerResponse.setError(e);
        AdminSparkServer.handleError(e, request, response);
      }
      return AdminSparkServer.OBJECT_MAPPER.writeValueAsString(controllerResponse);
    };
  }
}
