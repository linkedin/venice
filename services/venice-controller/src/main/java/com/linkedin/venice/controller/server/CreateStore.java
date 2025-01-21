package com.linkedin.venice.controller.server;

import static com.linkedin.venice.controller.server.VeniceControllerRequestHandler.DEFAULT_STORE_OWNER;
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
import com.linkedin.venice.protocols.controller.ClusterStoreGrpcInfo;
import com.linkedin.venice.protocols.controller.CreateStoreGrpcRequest;
import com.linkedin.venice.protocols.controller.CreateStoreGrpcResponse;
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
  public Route createStore(Admin admin, VeniceControllerRequestHandler requestHandler) {
    return new VeniceRouteHandler<NewStoreResponse>(NewStoreResponse.class) {
      @Override
      public void internalHandle(Request request, NewStoreResponse veniceResponse) {
        // Only allow allowlist users to run this command
        if (!checkIsAllowListUser(request, veniceResponse, () -> isAllowListUser(request))) {
          return;
        }
        // Validate request parameters
        AdminSparkServer.validateParams(request, NEW_STORE.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        String keySchema = request.queryParams(KEY_SCHEMA);
        String valueSchema = request.queryParams(VALUE_SCHEMA);
        boolean isSystemStore = Boolean.parseBoolean(request.queryParams(IS_SYSTEM_STORE));
        String owner = AdminSparkServer.getOptionalParameterValue(request, OWNER);
        if (owner == null) {
          owner = DEFAULT_STORE_OWNER;
        }
        String accessPerm = request.queryParams(ACCESS_PERMISSION);

        CreateStoreGrpcRequest.Builder requestBuilder = CreateStoreGrpcRequest.newBuilder()
            .setClusterStoreInfo(ClusterStoreGrpcInfo.newBuilder().setClusterName(clusterName).setStoreName(storeName))
            .setKeySchema(keySchema)
            .setValueSchema(valueSchema)
            .setOwner(owner)
            .setIsSystemStore(isSystemStore);
        if (accessPerm != null) {
          requestBuilder.setAccessPermission(accessPerm);
        }
        CreateStoreGrpcResponse internalResponse = requestHandler.createStore(requestBuilder.build());
        veniceResponse.setCluster(internalResponse.getClusterStoreInfo().getClusterName());
        veniceResponse.setName(internalResponse.getClusterStoreInfo().getStoreName());
        veniceResponse.setOwner(internalResponse.getOwner());
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
