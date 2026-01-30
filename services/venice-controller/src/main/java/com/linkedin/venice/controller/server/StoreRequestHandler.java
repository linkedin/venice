package com.linkedin.venice.controller.server;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.ControllerRequestHandlerDependencies;
import com.linkedin.venice.controller.StoreDeletedValidation;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.ZKStore;
import com.linkedin.venice.protocols.controller.ClusterStoreGrpcInfo;
import com.linkedin.venice.protocols.controller.CreateStoreGrpcRequest;
import com.linkedin.venice.protocols.controller.CreateStoreGrpcResponse;
import com.linkedin.venice.protocols.controller.DeleteAclForStoreGrpcRequest;
import com.linkedin.venice.protocols.controller.DeleteAclForStoreGrpcResponse;
import com.linkedin.venice.protocols.controller.GetAclForStoreGrpcRequest;
import com.linkedin.venice.protocols.controller.GetAclForStoreGrpcResponse;
import com.linkedin.venice.protocols.controller.ListStoresGrpcRequest;
import com.linkedin.venice.protocols.controller.ListStoresGrpcResponse;
import com.linkedin.venice.protocols.controller.UpdateAclForStoreGrpcRequest;
import com.linkedin.venice.protocols.controller.UpdateAclForStoreGrpcResponse;
import com.linkedin.venice.protocols.controller.ValidateStoreDeletedGrpcRequest;
import com.linkedin.venice.protocols.controller.ValidateStoreDeletedGrpcResponse;
import com.linkedin.venice.systemstore.schemas.StoreProperties;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.avro.Schema;
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

  /**
   * Validates whether a store has been successfully deleted.
   * @param request the request containing cluster and store name
   * @return response indicating if store is deleted and reason if not
   */
  public ValidateStoreDeletedGrpcResponse validateStoreDeleted(ValidateStoreDeletedGrpcRequest request) {
    ClusterStoreGrpcInfo storeInfo = request.getStoreInfo();
    ControllerRequestParamValidator.validateClusterStoreInfo(storeInfo);
    String clusterName = storeInfo.getClusterName();
    String storeName = storeInfo.getStoreName();
    LOGGER.info("Validating store deleted for store: {} in cluster: {}", storeName, clusterName);
    StoreDeletedValidation result = admin.validateStoreDeleted(clusterName, storeName);
    ValidateStoreDeletedGrpcResponse.Builder responseBuilder =
        ValidateStoreDeletedGrpcResponse.newBuilder().setStoreInfo(storeInfo).setStoreDeleted(result.isDeleted());
    if (result.getError() != null) {
      responseBuilder.setReason(result.getError());
    }
    return responseBuilder.build();
  }

  /**
   * Lists all stores in the specified cluster with optional filtering.
   * @param request the request containing cluster name and optional filters
   * @return response containing the list of store names
   */
  public ListStoresGrpcResponse listStores(ListStoresGrpcRequest request) {
    String clusterName = request.getClusterName();
    if (StringUtils.isBlank(clusterName)) {
      throw new IllegalArgumentException("Cluster name is required");
    }

    boolean includeSystemStores = !request.hasIncludeSystemStores() || request.getIncludeSystemStores();
    Optional<String> storeConfigNameFilter =
        request.hasStoreConfigNameFilter() ? Optional.of(request.getStoreConfigNameFilter()) : Optional.empty();
    Optional<String> storeConfigValueFilter =
        request.hasStoreConfigValueFilter() ? Optional.of(request.getStoreConfigValueFilter()) : Optional.empty();

    if (storeConfigNameFilter.isPresent() ^ storeConfigValueFilter.isPresent()) {
      throw new VeniceException(
          "Missing parameter: "
              + (storeConfigNameFilter.isPresent() ? "store_config_value_filter" : "store_config_name_filter"));
    }

    boolean isDataReplicationPolicyConfigFilter = false;
    Schema.Field configFilterField = null;
    if (storeConfigNameFilter.isPresent()) {
      configFilterField = StoreProperties.getClassSchema().getField(storeConfigNameFilter.get());
      if (configFilterField == null) {
        isDataReplicationPolicyConfigFilter = storeConfigNameFilter.get().equalsIgnoreCase("dataReplicationPolicy");
        if (!isDataReplicationPolicyConfigFilter) {
          throw new VeniceException(
              "The config name filter " + storeConfigNameFilter.get() + " is not a valid store config.");
        }
      }
    }

    LOGGER.info(
        "Listing stores in cluster: {} with includeSystemStores: {}, configNameFilter: {}, configValueFilter: {}",
        clusterName,
        includeSystemStores,
        storeConfigNameFilter.orElse("none"),
        storeConfigValueFilter.orElse("none"));

    List<Store> storeList = admin.getAllStores(clusterName);
    List<String> selectedStoreNames = new ArrayList<>();

    for (Store store: storeList) {
      if (!includeSystemStores && store.isSystemStore()) {
        continue;
      }
      if (storeConfigValueFilter.isPresent()) {
        boolean configValueMatch = false;
        if (isDataReplicationPolicyConfigFilter) {
          if (!store.isHybrid() || store.getHybridStoreConfig().getDataReplicationPolicy() == null) {
            continue;
          }
          configValueMatch = store.getHybridStoreConfig()
              .getDataReplicationPolicy()
              .name()
              .equalsIgnoreCase(storeConfigValueFilter.get());
        } else {
          ZKStore cloneStore = new ZKStore(store);
          Object configValue = cloneStore.dataModel().get(storeConfigNameFilter.get());
          if (configValue == null) {
            continue;
          }
          Schema fieldSchema = configFilterField.schema();
          switch (fieldSchema.getType()) {
            case BOOLEAN:
              configValueMatch = Boolean.valueOf(storeConfigValueFilter.get()).equals((Boolean) configValue);
              break;
            case INT:
              configValueMatch = Integer.valueOf(storeConfigValueFilter.get()).equals((Integer) configValue);
              break;
            case LONG:
              configValueMatch = Long.valueOf(storeConfigValueFilter.get()).equals((Long) configValue);
              break;
            case FLOAT:
              configValueMatch = Float.valueOf(storeConfigValueFilter.get()).equals(configValue);
              break;
            case DOUBLE:
              configValueMatch = Double.valueOf(storeConfigValueFilter.get()).equals(configValue);
              break;
            case STRING:
              configValueMatch = storeConfigValueFilter.get().equals(configValue);
              break;
            case ENUM:
              configValueMatch = storeConfigValueFilter.get().equals(configValue.toString());
              break;
            case UNION:
              configValueMatch = (configValue != null);
              break;
            case ARRAY:
            case MAP:
            case FIXED:
            case BYTES:
            case RECORD:
            case NULL:
            default:
              throw new VeniceException(
                  "Store config filtering for Schema type " + fieldSchema.getType().toString() + " is not supported");
          }
        }
        if (!configValueMatch) {
          continue;
        }
      }
      selectedStoreNames.add(store.getName());
    }

    LOGGER.info("Found {} stores in cluster: {}", selectedStoreNames.size(), clusterName);
    return ListStoresGrpcResponse.newBuilder().setClusterName(clusterName).addAllStoreNames(selectedStoreNames).build();
  }

  /**
   * Gets store information for a given store in a cluster.
   * @param clusterName the name of the cluster
   * @param storeName the name of the store
   * @return StoreResponse containing store information
   */
  public StoreResponse getStore(String clusterName, String storeName) {
    ControllerRequestParamValidator.validateClusterAndStoreName(clusterName, storeName);

    LOGGER.info("Getting store info for store: {} in cluster: {}", storeName, clusterName);

    Store store = admin.getStore(clusterName, storeName);
    if (store == null) {
      throw new VeniceNoStoreException(storeName);
    }

    StoreInfo storeInfo = StoreInfo.fromStore(store);
    // Set default retention time if not set
    if (storeInfo.getBackupVersionRetentionMs() < 0) {
      storeInfo.setBackupVersionRetentionMs(admin.getBackupVersionDefaultRetentionMs());
    }
    // Set default max record size if not set
    if (storeInfo.getMaxRecordSizeBytes() < 0) {
      storeInfo.setMaxRecordSizeBytes(admin.getDefaultMaxRecordSizeBytes());
    }
    storeInfo.setColoToCurrentVersions(admin.getCurrentVersionsForMultiColos(clusterName, storeName));
    boolean isSSL = admin.isSSLEnabledForPush(clusterName, storeName);
    storeInfo.setKafkaBrokerUrl(admin.getKafkaBootstrapServers(isSSL));

    StoreResponse response = new StoreResponse();
    response.setCluster(clusterName);
    response.setName(storeName);
    response.setStore(storeInfo);
    return response;
  }
}
