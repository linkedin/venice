package com.linkedin.venice.controller.server;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.ControllerRequestHandlerDependencies;
import com.linkedin.venice.protocols.controller.ClusterStoreGrpcInfo;
import com.linkedin.venice.protocols.controller.CreateStoreGrpcRequest;
import com.linkedin.venice.protocols.controller.CreateStoreGrpcResponse;
import com.linkedin.venice.protocols.controller.DeleteAclForStoreGrpcRequest;
import com.linkedin.venice.protocols.controller.DeleteAclForStoreGrpcResponse;
import com.linkedin.venice.protocols.controller.GetAclForStoreGrpcRequest;
import com.linkedin.venice.protocols.controller.GetAclForStoreGrpcResponse;
import com.linkedin.venice.protocols.controller.UpdateAclForStoreGrpcRequest;
import com.linkedin.venice.protocols.controller.UpdateAclForStoreGrpcResponse;
import java.util.Optional;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class StoreRequestHandler {
  public static final String DEFAULT_STORE_OWNER = "";
  Logger LOGGER = LogManager.getLogger(StoreRequestHandler.class);

  private final Admin admin;

  public StoreRequestHandler(ControllerRequestHandlerDependencies dependencies) {
    this.admin = dependencies.getAdmin();
  }

  /**
   * Creates a new store in the specified Venice cluster with the provided parameters.
   * @param request the request object containing all necessary details for the creation of the store
   */
  public CreateStoreGrpcResponse createStore(CreateStoreGrpcRequest request) {
    ClusterStoreGrpcInfo clusterStoreInfo = request.getStoreInfo();
    String clusterName = clusterStoreInfo.getClusterName();
    String storeName = clusterStoreInfo.getStoreName();
    String keySchema = request.getKeySchema();
    String valueSchema = request.getValueSchema();
    String owner = request.hasOwner() ? request.getOwner() : null;
    if (owner == null) {
      owner = DEFAULT_STORE_OWNER;
    }
    Optional<String> accessPermissions =
        Optional.ofNullable(request.hasAccessPermission() ? request.getAccessPermission() : null);
    boolean isSystemStore = request.hasIsSystemStore() && request.getIsSystemStore();
    ControllerRequestParamValidator.createStoreRequestValidator(clusterName, storeName, owner, keySchema, valueSchema);
    LOGGER.info(
        "Creating store: {} in cluster: {} with owner: {} and key schema: {} and value schema: {} and isSystemStore: {} and access permissions: {}",
        storeName,
        clusterName,
        owner,
        keySchema,
        valueSchema,
        isSystemStore,
        accessPermissions);
    admin.createStore(clusterName, storeName, owner, keySchema, valueSchema, isSystemStore, accessPermissions);
    CreateStoreGrpcResponse.Builder responseBuilder =
        CreateStoreGrpcResponse.newBuilder().setStoreInfo(clusterStoreInfo).setOwner(owner);

    LOGGER.info("Successfully created store: {} in cluster: {}", storeName, clusterName);
    return responseBuilder.build();
  }

  public UpdateAclForStoreGrpcResponse updateAclForStore(UpdateAclForStoreGrpcRequest request) {
    ControllerRequestParamValidator.validateClusterStoreInfo(request.getStoreInfo());
    String accessPermissions = request.getAccessPermissions();
    if (StringUtils.isBlank(accessPermissions)) {
      throw new IllegalArgumentException("Access permissions is required for updating ACL");
    }
    ClusterStoreGrpcInfo storeInfo = request.getStoreInfo();
    String cluster = storeInfo.getClusterName();
    String storeName = storeInfo.getStoreName();
    LOGGER.info(
        "Updating ACL for store: {} in cluster: {} with access permissions: {}",
        storeName,
        cluster,
        accessPermissions);
    admin.updateAclForStore(cluster, storeName, accessPermissions);
    LOGGER.info("Successfully updated ACL for store: {} in cluster: {}", storeName, cluster);
    return UpdateAclForStoreGrpcResponse.newBuilder().setStoreInfo(storeInfo).build();
  }

  public GetAclForStoreGrpcResponse getAclForStore(GetAclForStoreGrpcRequest request) {
    ControllerRequestParamValidator.validateClusterStoreInfo(request.getStoreInfo());
    ClusterStoreGrpcInfo storeInfo = request.getStoreInfo();
    LOGGER.info("Getting ACL for store: {} in cluster: {}", storeInfo.getStoreName(), storeInfo.getClusterName());
    String accessPermissions = admin.getAclForStore(storeInfo.getClusterName(), storeInfo.getStoreName());
    GetAclForStoreGrpcResponse.Builder builder = GetAclForStoreGrpcResponse.newBuilder().setStoreInfo(storeInfo);
    if (accessPermissions != null) {
      builder.setAccessPermissions(accessPermissions);
    }
    return builder.build();
  }

  public DeleteAclForStoreGrpcResponse deleteAclForStore(DeleteAclForStoreGrpcRequest request) {
    ControllerRequestParamValidator.validateClusterStoreInfo(request.getStoreInfo());
    ClusterStoreGrpcInfo storeInfo = request.getStoreInfo();
    LOGGER.info("Deleting ACL for store: {} in cluster: {}", storeInfo.getStoreName(), storeInfo.getClusterName());
    admin.deleteAclForStore(storeInfo.getClusterName(), storeInfo.getStoreName());
    return DeleteAclForStoreGrpcResponse.newBuilder().setStoreInfo(storeInfo).build();
  }

  public void checkResourceCleanupForStoreCreation(ClusterStoreGrpcInfo request) {
    ControllerRequestParamValidator.validateClusterStoreInfo(request);
    LOGGER.info(
        "Checking resource cleanup for store: {} in cluster: {}",
        request.getStoreName(),
        request.getClusterName());
    admin.checkResourceCleanupBeforeStoreCreation(request.getClusterName(), request.getStoreName());
  }
}
