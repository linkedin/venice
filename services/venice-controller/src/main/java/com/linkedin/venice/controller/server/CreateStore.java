package com.linkedin.venice.controller.server;

import static com.linkedin.venice.controller.server.StoreRequestHandler.DEFAULT_STORE_OWNER;
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
import com.linkedin.venice.protocols.controller.DeleteAclForStoreGrpcRequest;
import com.linkedin.venice.protocols.controller.GetAclForStoreGrpcRequest;
import com.linkedin.venice.protocols.controller.GetAclForStoreGrpcResponse;
import com.linkedin.venice.protocols.controller.UpdateAclForStoreGrpcRequest;
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
  public Route createStore(Admin admin, StoreRequestHandler requestHandler) {
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

        ClusterStoreGrpcInfo storeInfo =
            ClusterStoreGrpcInfo.newBuilder().setClusterName(clusterName).setStoreName(storeName).build();
        CreateStoreGrpcRequest.Builder requestBuilder =
            CreateStoreGrpcRequest.newBuilder().setStoreInfo(storeInfo).setOwner(owner).setIsSystemStore(isSystemStore);
        if (keySchema != null) {
          requestBuilder.setKeySchema(keySchema);
        }
        if (valueSchema != null) {
          requestBuilder.setValueSchema(valueSchema);
        }
        if (accessPerm != null) {
          requestBuilder.setAccessPermission(accessPerm);
        }
        CreateStoreGrpcResponse internalResponse = requestHandler.createStore(requestBuilder.build());
        veniceResponse.setCluster(internalResponse.getStoreInfo().getClusterName());
        veniceResponse.setName(internalResponse.getStoreInfo().getStoreName());
        veniceResponse.setOwner(internalResponse.getOwner());
      }
    };
  }

  /**
   * @see Admin#updateAclForStore(String, String, String)
   */
  public Route updateAclForStore(Admin admin, StoreRequestHandler requestHandler) {
    return (request, response) -> {
      AclResponse responseObject = new AclResponse();
      response.type(HttpConstants.JSON);
      try {
        // TODO need security validation here?
        AdminSparkServer.validateParams(request, UPDATE_ACL.getParams(), admin);
        String cluster = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        String accessPermissions = request.queryParams(ACCESS_PERMISSION);
        ClusterStoreGrpcInfo storeInfo =
            ClusterStoreGrpcInfo.newBuilder().setClusterName(cluster).setStoreName(storeName).build();
        UpdateAclForStoreGrpcRequest.Builder requestBuilder =
            UpdateAclForStoreGrpcRequest.newBuilder().setStoreInfo(storeInfo);
        if (accessPermissions != null) {
          requestBuilder.setAccessPermissions(accessPermissions);
        }
        requestHandler.updateAclForStore(requestBuilder.build());
        responseObject.setCluster(cluster);
        responseObject.setName(storeName);
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
  public Route getAclForStore(Admin admin, StoreRequestHandler requestHandler) {
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
        ClusterStoreGrpcInfo storeInfo =
            ClusterStoreGrpcInfo.newBuilder().setClusterName(cluster).setStoreName(storeName).build();
        GetAclForStoreGrpcRequest internalRequest =
            GetAclForStoreGrpcRequest.newBuilder().setStoreInfo(storeInfo).build();
        GetAclForStoreGrpcResponse internalResponse = requestHandler.getAclForStore(internalRequest);
        responseObject.setAccessPermissions(internalResponse.getAccessPermissions());
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
  public Route deleteAclForStore(Admin admin, StoreRequestHandler requestHandler) {
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
        ClusterStoreGrpcInfo storeInfo =
            ClusterStoreGrpcInfo.newBuilder().setClusterName(cluster).setStoreName(storeName).build();
        requestHandler.deleteAclForStore(DeleteAclForStoreGrpcRequest.newBuilder().setStoreInfo(storeInfo).build());
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
  public Route checkResourceCleanupForStoreCreation(Admin admin, StoreRequestHandler requestHandler) {
    return (request, response) -> {
      ControllerResponse controllerResponse = new ControllerResponse();
      response.type(HttpConstants.JSON);
      try {
        AdminSparkServer.validateParams(request, GET_ACL.getParams(), admin);
        String cluster = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        controllerResponse.setCluster(cluster);
        controllerResponse.setName(storeName);
        requestHandler.checkResourceCleanupForStoreCreation(
            ClusterStoreGrpcInfo.newBuilder().setClusterName(cluster).setStoreName(storeName).build());
      } catch (Throwable e) {
        controllerResponse.setError(e);
        AdminSparkServer.handleError(e, request, response);
      }
      return AdminSparkServer.OBJECT_MAPPER.writeValueAsString(controllerResponse);
    };
  }

}
