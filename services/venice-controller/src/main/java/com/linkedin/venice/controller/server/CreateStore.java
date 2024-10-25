package com.linkedin.venice.controller.server;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.ACCESS_PERMISSION;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.CLUSTER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.IS_SYSTEM_STORE;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.KEY_SCHEMA;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.NAME;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.OWNER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.VALUE_SCHEMA;
import static com.linkedin.venice.controllerapi.ControllerRoute.DELETE_ACL;
import static com.linkedin.venice.controllerapi.ControllerRoute.GET_ACL;
import static com.linkedin.venice.controllerapi.ControllerRoute.NEW_STORE;
import static com.linkedin.venice.controllerapi.ControllerRoute.UPDATE_ACL;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.AclResponse;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.request.ControllerRequest;
import com.linkedin.venice.controllerapi.request.NewStoreRequest;
import com.linkedin.venice.controllerapi.request.UpdateAclForStoreRequest;
import java.util.Optional;
import spark.Request;
import spark.Route;


public class CreateStore extends AbstractRoute {
  public CreateStore(
      boolean sslEnabled,
      Optional<DynamicAccessController> accessController,
      VeniceControllerRequestHandler requestHandler) {
    super(sslEnabled, accessController, requestHandler);
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
        // Validate request parameters
        AdminSparkServer.validateParams(request, NEW_STORE.getParams(), admin);
        // Extract the parameters from the spark request and create the generic request object
        NewStoreRequest storeRequest = new NewStoreRequest(
            request.queryParams(CLUSTER),
            request.queryParams(NAME),
            AdminSparkServer.getOptionalParameterValue(request, OWNER),
            request.queryParams(KEY_SCHEMA),
            request.queryParams(VALUE_SCHEMA),
            request.queryParams(ACCESS_PERMISSION),
            Boolean.parseBoolean(request.queryParams(IS_SYSTEM_STORE)));
        requestHandler.createStore(storeRequest, veniceResponse);
      }
    };
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
        UpdateAclForStoreRequest updateAclForStoreRequest = new UpdateAclForStoreRequest(
            request.queryParams(CLUSTER),
            request.queryParams(NAME),
            request.queryParams(ACCESS_PERMISSION));
        requestHandler.updateAclForStore(updateAclForStoreRequest, responseObject);
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
        requestHandler.getAclForStore(
            new ControllerRequest(request.queryParams(CLUSTER), request.queryParams(NAME)),
            responseObject);
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
        requestHandler.deleteAclForStore(
            new ControllerRequest(request.queryParams(CLUSTER), request.queryParams(NAME)),
            responseObject);
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
        requestHandler.checkResourceCleanupBeforeStoreCreation(
            new ControllerRequest(request.queryParams(CLUSTER), request.queryParams(NAME)),
            controllerResponse);
      } catch (Throwable e) {
        controllerResponse.setError(e);
        AdminSparkServer.handleError(e, request, response);
      }
      return AdminSparkServer.OBJECT_MAPPER.writeValueAsString(controllerResponse);
    };
  }
}
