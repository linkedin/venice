package com.linkedin.venice.controller.server;

import static com.linkedin.venice.controller.server.VeniceRouteHandler.ACL_CHECK_FAILURE_WARN_MESSAGE_PREFIX;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.CLUSTER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.CLUSTER_DEST;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.FABRIC;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.FABRIC_A;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.FABRIC_B;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.HEARTBEAT_TIMESTAMP;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.INCLUDE_SYSTEM_STORES;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.NAME;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.NATIVE_REPLICATION_SOURCE_FABRIC;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.OPERATION;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.OWNER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.PARTITION_DETAIL_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.READ_OPERATION;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.READ_WRITE_OPERATION;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.REGIONS_FILTER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.STATUS;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.STORE_CONFIG_NAME_FILTER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.STORE_CONFIG_VALUE_FILTER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.STORE_TYPE;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.TOPIC;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.TOPIC_COMPACTION_POLICY;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.VERSION;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.WRITE_OPERATION;
import static com.linkedin.venice.controllerapi.ControllerRoute.ABORT_MIGRATION;
import static com.linkedin.venice.controllerapi.ControllerRoute.CLUSTER_HEALTH_STORES;
import static com.linkedin.venice.controllerapi.ControllerRoute.COMPARE_STORE;
import static com.linkedin.venice.controllerapi.ControllerRoute.COMPLETE_MIGRATION;
import static com.linkedin.venice.controllerapi.ControllerRoute.CONFIGURE_ACTIVE_ACTIVE_REPLICATION_FOR_CLUSTER;
import static com.linkedin.venice.controllerapi.ControllerRoute.CONFIGURE_NATIVE_REPLICATION_FOR_CLUSTER;
import static com.linkedin.venice.controllerapi.ControllerRoute.DELETE_ALL_VERSIONS;
import static com.linkedin.venice.controllerapi.ControllerRoute.DELETE_KAFKA_TOPIC;
import static com.linkedin.venice.controllerapi.ControllerRoute.DELETE_STORE;
import static com.linkedin.venice.controllerapi.ControllerRoute.ENABLE_STORE;
import static com.linkedin.venice.controllerapi.ControllerRoute.FUTURE_VERSION;
import static com.linkedin.venice.controllerapi.ControllerRoute.GET_DELETABLE_STORE_TOPICS;
import static com.linkedin.venice.controllerapi.ControllerRoute.GET_HEARTBEAT_TIMESTAMP_FROM_SYSTEM_STORE;
import static com.linkedin.venice.controllerapi.ControllerRoute.GET_REGION_PUSH_DETAILS;
import static com.linkedin.venice.controllerapi.ControllerRoute.GET_REPUSH_INFO;
import static com.linkedin.venice.controllerapi.ControllerRoute.GET_STALE_STORES_IN_CLUSTER;
import static com.linkedin.venice.controllerapi.ControllerRoute.GET_STORES_IN_CLUSTER;
import static com.linkedin.venice.controllerapi.ControllerRoute.LIST_STORES;
import static com.linkedin.venice.controllerapi.ControllerRoute.LIST_STORE_PUSH_INFO;
import static com.linkedin.venice.controllerapi.ControllerRoute.MIGRATE_STORE;
import static com.linkedin.venice.controllerapi.ControllerRoute.REMOVE_STORE_FROM_GRAVEYARD;
import static com.linkedin.venice.controllerapi.ControllerRoute.ROLLBACK_TO_BACKUP_VERSION;
import static com.linkedin.venice.controllerapi.ControllerRoute.ROLL_FORWARD_TO_FUTURE_VERSION;
import static com.linkedin.venice.controllerapi.ControllerRoute.SEND_HEARTBEAT_TIMESTAMP_TO_SYSTEM_STORE;
import static com.linkedin.venice.controllerapi.ControllerRoute.SET_OWNER;
import static com.linkedin.venice.controllerapi.ControllerRoute.SET_TOPIC_COMPACTION;
import static com.linkedin.venice.controllerapi.ControllerRoute.SET_VERSION;
import static com.linkedin.venice.controllerapi.ControllerRoute.STORAGE_ENGINE_OVERHEAD_RATIO;
import static com.linkedin.venice.controllerapi.ControllerRoute.STORE;
import static com.linkedin.venice.controllerapi.ControllerRoute.UPDATE_STORE;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.AdminCommandExecutionTracker;
import com.linkedin.venice.controller.kafka.TopicCleanupService;
import com.linkedin.venice.controllerapi.ClusterStaleDataAuditResponse;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.MultiStoreInfoResponse;
import com.linkedin.venice.controllerapi.MultiStoreResponse;
import com.linkedin.venice.controllerapi.MultiStoreStatusResponse;
import com.linkedin.venice.controllerapi.MultiStoreTopicsResponse;
import com.linkedin.venice.controllerapi.MultiVersionResponse;
import com.linkedin.venice.controllerapi.OwnerResponse;
import com.linkedin.venice.controllerapi.PartitionResponse;
import com.linkedin.venice.controllerapi.RegionPushDetailsResponse;
import com.linkedin.venice.controllerapi.RepushInfo;
import com.linkedin.venice.controllerapi.RepushInfoResponse;
import com.linkedin.venice.controllerapi.StorageEngineOverheadRatioResponse;
import com.linkedin.venice.controllerapi.StoreComparisonInfo;
import com.linkedin.venice.controllerapi.StoreComparisonResponse;
import com.linkedin.venice.controllerapi.StoreHealthAuditResponse;
import com.linkedin.venice.controllerapi.StoreMigrationResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.SystemStoreHeartbeatResponse;
import com.linkedin.venice.controllerapi.TrackableControllerResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionResponse;
import com.linkedin.venice.exceptions.ErrorType;
import com.linkedin.venice.exceptions.ResourceStillExistsException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.RegionPushDetails;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreDataAudit;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.VeniceUserStoreType;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.ZKStore;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.exceptions.PubSubTopicDoesNotExistException;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.systemstore.schemas.StoreProperties;
import com.linkedin.venice.utils.Utils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.http.HttpStatus;
import spark.Request;
import spark.Route;


public class StoresRoutes extends AbstractRoute {
  private final PubSubTopicRepository pubSubTopicRepository;

  public StoresRoutes(
      boolean sslEnabled,
      Optional<DynamicAccessController> accessController,
      PubSubTopicRepository pubSubTopicRepository) {
    super(sslEnabled, accessController);
    this.pubSubTopicRepository = pubSubTopicRepository;
  }

  /**
   * No ACL check; any user can try to list stores. If we get abused in future, we should only allow Venice admins
   * to run this command.
   * @see Admin#getAllStores(String)
   */
  public Route getAllStores(Admin admin) {
    return new VeniceRouteHandler<MultiStoreResponse>(MultiStoreResponse.class) {
      @Override
      public void internalHandle(Request request, MultiStoreResponse veniceResponse) {
        AdminSparkServer.validateParams(request, LIST_STORES.getParams(), admin);
        veniceResponse.setCluster(request.queryParams(CLUSTER));
        veniceResponse.setName(request.queryParams(NAME));

        // Potentially filter out the system stores
        String includeSystemStores = request.queryParams(INCLUDE_SYSTEM_STORES);
        // If the param is not provided, the default is to include them
        boolean excludeSystemStores = (includeSystemStores != null && !Boolean.parseBoolean(includeSystemStores));
        Optional<String> storeConfigNameFilter =
            Optional.ofNullable(request.queryParamOrDefault(STORE_CONFIG_NAME_FILTER, null));
        Optional<String> storeConfigValueFilter =
            Optional.ofNullable(request.queryParamOrDefault(STORE_CONFIG_VALUE_FILTER, null));
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

        List<Store> storeList = admin.getAllStores(veniceResponse.getCluster());
        List<Store> selectedStoreList;
        if (excludeSystemStores || storeConfigNameFilter.isPresent()) {
          selectedStoreList = new ArrayList<>();
          for (Store store: storeList) {
            if (excludeSystemStores && store.isSystemStore()) {
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
                  // If the store doesn't have the config, it fails the match
                  continue;
                }
                // Compare based on schema type
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
                    /**
                     * For union field, return match as long as the union field is not null
                     */
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
                        "Store config filtering for Schema type " + fieldSchema.getType().toString()
                            + " is not supported");
                }
              }
              if (!configValueMatch) {
                continue;
              }
            }
            selectedStoreList.add(store);
          }
        } else {
          selectedStoreList = storeList;
        }

        String[] storeNameList = new String[selectedStoreList.size()];
        for (int i = 0; i < selectedStoreList.size(); i++) {
          storeNameList[i] = selectedStoreList.get(i).getName();
        }
        veniceResponse.setStores(storeNameList);
      }
    };
  }

  /**
   * No ACL check; any user can try to list store statuses.
   * @see Admin#getAllStoreStatuses(String)
   */
  public Route getAllStoresStatuses(Admin admin) {
    return new VeniceRouteHandler<MultiStoreStatusResponse>(MultiStoreStatusResponse.class) {
      @Override
      public void internalHandle(Request request, MultiStoreStatusResponse veniceResponse) {
        AdminSparkServer.validateParams(request, CLUSTER_HEALTH_STORES.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);
        veniceResponse.setCluster(clusterName);
        Map<String, String> storeStatusMap = admin.getAllStoreStatuses(clusterName);
        veniceResponse.setStoreStatusMap(storeStatusMap);
      }
    };
  }

  /**
   * @see Admin#getRepushInfo(String, String, Optional)
   */
  public Route getRepushInfo(Admin admin) {
    return new VeniceRouteHandler<RepushInfoResponse>(RepushInfoResponse.class) {
      @Override
      public void internalHandle(Request request, RepushInfoResponse veniceResponse) {
        AdminSparkServer.validateParams(request, GET_REPUSH_INFO.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        String fabricName = request.queryParams(FABRIC);

        veniceResponse.setCluster(clusterName);
        veniceResponse.setName(storeName);

        RepushInfo repushInfo = admin.getRepushInfo(clusterName, storeName, Optional.ofNullable(fabricName));

        veniceResponse.setRepushInfo(repushInfo);
      }
    };
  }

  /**
   * @see Admin#getStore(String, String)
   */
  public Route getStore(Admin admin) {
    return new VeniceRouteHandler<StoreResponse>(StoreResponse.class) {
      @Override
      public void internalHandle(Request request, StoreResponse veniceResponse) {
        // No ACL check for getting store metadata
        AdminSparkServer.validateParams(request, STORE.getParams(), admin);
        String storeName = request.queryParams(NAME);
        String clusterName = request.queryParams(CLUSTER);
        veniceResponse.setCluster(clusterName);
        veniceResponse.setName(storeName);
        Store store = admin.getStore(clusterName, storeName);
        if (store == null) {
          throw new VeniceNoStoreException(storeName);
        }
        StoreInfo storeInfo = StoreInfo.fromStore(store);
        // Make sure store info will have right default retention time for Nuage UI display.
        if (storeInfo.getBackupVersionRetentionMs() < 0) {
          storeInfo.setBackupVersionRetentionMs(admin.getBackupVersionDefaultRetentionMs());
        }
        storeInfo.setColoToCurrentVersions(admin.getCurrentVersionsForMultiColos(clusterName, storeName));
        boolean isSSL = admin.isSSLEnabledForPush(clusterName, storeName);
        storeInfo.setKafkaBrokerUrl(admin.getKafkaBootstrapServers(isSSL));

        veniceResponse.setStore(storeInfo);
      }
    };
  }

  /**
   * @see Admin#getFutureVersion(String, String)
   */
  public Route getFutureVersion(Admin admin) {
    return new VeniceRouteHandler<MultiStoreStatusResponse>(MultiStoreStatusResponse.class) {
      @Override
      public void internalHandle(Request request, MultiStoreStatusResponse veniceResponse) {
        AdminSparkServer.validateParams(request, FUTURE_VERSION.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        veniceResponse.setCluster(clusterName);
        Store store = admin.getStore(clusterName, storeName);
        if (store == null) {
          throw new VeniceNoStoreException(storeName);
        }
        Map<String, String> storeStatusMap = admin.getFutureVersionsForMultiColos(clusterName, storeName);
        if (storeStatusMap.isEmpty()) {
          // Non parent controllers will return an empty map, so we'll just return the childs version of this api
          storeStatusMap =
              Collections.singletonMap(storeName, String.valueOf(admin.getFutureVersion(clusterName, storeName)));
        }
        veniceResponse.setStoreStatusMap(storeStatusMap);
      }
    };
  }

  /**
   * @see Admin#migrateStore(String, String, String)
   */
  public Route migrateStore(Admin admin) {
    return new VeniceRouteHandler<StoreMigrationResponse>(StoreMigrationResponse.class) {
      @Override
      public void internalHandle(Request request, StoreMigrationResponse veniceResponse) {
        // Only allow allowlist users to run this command
        if (!checkIsAllowListUser(request, veniceResponse, () -> isAllowListUser(request))) {
          return;
        }
        AdminSparkServer.validateParams(request, MIGRATE_STORE.getParams(), admin);
        String srcClusterName = request.queryParams(CLUSTER);
        String destClusterName = request.queryParams(CLUSTER_DEST);
        String storeName = request.queryParams(NAME);

        veniceResponse.setSrcClusterName(srcClusterName);
        veniceResponse.setCluster(destClusterName);
        veniceResponse.setName(storeName);

        String clusterDiscovered = admin.discoverCluster(storeName).getFirst();
        // Store should belong to src cluster already
        if (!clusterDiscovered.equals(srcClusterName)) {
          veniceResponse.setError(
              "Store " + storeName + " belongs to cluster " + clusterDiscovered
                  + ", which is different from the given src cluster name " + srcClusterName);
          veniceResponse.setErrorType(ErrorType.BAD_REQUEST);
          return;
        }
        // Store should not belong to dest cluster already
        if (clusterDiscovered.equals(destClusterName)) {
          veniceResponse.setError("Store " + storeName + " already belongs to cluster " + destClusterName);
          veniceResponse.setErrorType(ErrorType.BAD_REQUEST);
          return;
        }

        admin.migrateStore(srcClusterName, destClusterName, storeName);
      }
    };
  }

  /**
   * @see Admin#completeMigration(String, String, String)
   */
  public Route completeMigration(Admin admin) {
    return new VeniceRouteHandler<StoreMigrationResponse>(StoreMigrationResponse.class) {
      @Override
      public void internalHandle(Request request, StoreMigrationResponse veniceResponse) {
        // Only allow allowlist users to run this command
        if (!checkIsAllowListUser(request, veniceResponse, () -> isAllowListUser(request))) {
          return;
        }
        AdminSparkServer.validateParams(request, COMPLETE_MIGRATION.getParams(), admin);
        String srcClusterName = request.queryParams(CLUSTER);
        String destClusterName = request.queryParams(CLUSTER_DEST);
        String storeName = request.queryParams(NAME);

        veniceResponse.setSrcClusterName(srcClusterName);
        veniceResponse.setCluster(destClusterName);
        veniceResponse.setName(storeName);

        String clusterDiscovered = admin.discoverCluster(storeName).getFirst();
        // Store should belong to src cluster already
        if (!clusterDiscovered.equals(srcClusterName)) {
          veniceResponse.setError(
              "Store " + storeName + " belongs to cluster " + clusterDiscovered
                  + ", which is different from the given src cluster name " + srcClusterName);
          veniceResponse.setErrorType(ErrorType.BAD_REQUEST);
          return;
        }
        // Store should not belong to dest cluster already
        if (clusterDiscovered.equals(destClusterName)) {
          veniceResponse.setError("Store " + storeName + " already belongs to cluster " + destClusterName);
          veniceResponse.setErrorType(ErrorType.BAD_REQUEST);
          return;
        }

        admin.completeMigration(srcClusterName, destClusterName, storeName);
      }
    };
  }

  /**
   * @see Admin#abortMigration(String, String, String)
   */
  public Route abortMigration(Admin admin) {
    return new VeniceRouteHandler<StoreMigrationResponse>(StoreMigrationResponse.class) {
      @Override
      public void internalHandle(Request request, StoreMigrationResponse veniceResponse) {
        try {
          // Only allow allowlist users to run this command
          if (!checkIsAllowListUser(request, veniceResponse, () -> isAllowListUser(request))) {
            return;
          }
          AdminSparkServer.validateParams(request, ABORT_MIGRATION.getParams(), admin);
          String srcClusterName = request.queryParams(CLUSTER);
          String destClusterName = request.queryParams(CLUSTER_DEST);
          String storeName = request.queryParams(NAME);

          veniceResponse.setName(storeName);

          String clusterDiscovered = admin.discoverCluster(storeName).getFirst();
          veniceResponse.setSrcClusterName(clusterDiscovered);

          admin.abortMigration(srcClusterName, destClusterName, storeName);

          clusterDiscovered = admin.discoverCluster(storeName).getFirst();
          veniceResponse.setCluster(clusterDiscovered);
        } catch (Throwable e) {
          veniceResponse.setError(e);
        }
      }
    };
  }

  /**
   * @see Admin#deleteStore(String, String, int, boolean)
   */
  public Route deleteStore(Admin admin) {
    return new VeniceRouteHandler<TrackableControllerResponse>(TrackableControllerResponse.class) {
      @Override
      public void internalHandle(Request request, TrackableControllerResponse veniceResponse) {
        // Only allow allowlist users to run this command
        if (!checkIsAllowListUser(request, veniceResponse, () -> isAllowListUser(request))) {
          return;
        }
        AdminSparkServer.validateParams(request, DELETE_STORE.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);

        veniceResponse.setCluster(clusterName);
        veniceResponse.setName(storeName);

        Optional<AdminCommandExecutionTracker> adminCommandExecutionTracker =
            admin.getAdminCommandExecutionTracker(clusterName);
        if (adminCommandExecutionTracker.isPresent()) {
          // Lock the tracker to get the execution id for the last admin command.
          // If will not make our performance worse, because we lock the whole cluster while handling the admin
          // operation in parent admin.
          synchronized (adminCommandExecutionTracker) {
            admin.deleteStore(clusterName, storeName, Store.IGNORE_VERSION, false);
            veniceResponse.setExecutionId(adminCommandExecutionTracker.get().getLastExecutionId());
          }
        } else {
          admin.deleteStore(clusterName, storeName, Store.IGNORE_VERSION, false);
        }
      }
    };
  }

  /**
   * @see Admin#updateStore(String, String, UpdateStoreQueryParams)
   */
  public Route updateStore(Admin admin) {
    return new VeniceRouteHandler<ControllerResponse>(ControllerResponse.class) {
      @Override
      public void internalHandle(Request request, ControllerResponse veniceResponse) {
        // Only allow allowlist users to run this command
        if (!checkIsAllowListUser(request, veniceResponse, () -> isAllowListUser(request))) {
          return;
        }
        AdminSparkServer.validateParams(request, UPDATE_STORE.getParams(), admin);
        // TODO: we may want to have a specific response for store updating
        String clusterName = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);

        veniceResponse.setCluster(clusterName);
        veniceResponse.setName(storeName);

        Map<String, String> params = Utils.extractQueryParamsFromRequest(request.queryMap().toMap(), veniceResponse);

        try {
          admin.updateStore(clusterName, storeName, new UpdateStoreQueryParams(params));
        } catch (Exception e) {
          veniceResponse.setError(
              "Failed when updating store " + storeName + ". Exception type: " + e.getClass().toString()
                  + ". Detailed message = " + e.getMessage(),
              e);
        }
      }
    };
  }

  /**
   * @see Admin#setStoreOwner(String, String, String)
   */
  public Route setOwner(Admin admin) {
    return new VeniceRouteHandler<OwnerResponse>(OwnerResponse.class) {
      @Override
      public void internalHandle(Request request, OwnerResponse veniceResponse) {
        // Only admin users are allowed to update owners; regular user can do it through Nuage
        if (!isAllowListUser(request)) {
          veniceResponse.setError("ACL failed for request " + request.url());
          veniceResponse.setErrorType(ErrorType.BAD_REQUEST);
          return;
        }
        AdminSparkServer.validateParams(request, SET_OWNER.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        String owner = request.queryParams(OWNER);
        admin.setStoreOwner(clusterName, storeName, owner);

        veniceResponse.setCluster(clusterName);
        veniceResponse.setName(storeName);
        veniceResponse.setOwner(owner);
      }
    };
  }

  public Route setPartitionCount(Admin admin) {
    return new VeniceRouteHandler<PartitionResponse>(PartitionResponse.class) {
      @Override
      public void internalHandle(Request request, PartitionResponse veniceResponse) {
        veniceResponse.setError("This operation is no longer supported, please use the update store endpoint");
        veniceResponse.setErrorType(ErrorType.BAD_REQUEST);
      }
    };
  }

  /**
   * @see Admin#setStoreCurrentVersion(String, String, int)
   */
  public Route setCurrentVersion(Admin admin) {
    return new VeniceRouteHandler<VersionResponse>(VersionResponse.class) {
      @Override
      public void internalHandle(Request request, VersionResponse veniceResponse) {
        // Only allow allowlist users to run this command
        if (!checkIsAllowListUser(request, veniceResponse, () -> isAllowListUser(request))) {
          return;
        }
        AdminSparkServer.validateParams(request, SET_VERSION.getParams(), admin); // throws venice exception
        String clusterName = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        int version = Utils.parseIntFromString(request.queryParams(VERSION), VERSION);
        admin.setStoreCurrentVersion(clusterName, storeName, version);

        veniceResponse.setCluster(clusterName);
        veniceResponse.setName(storeName);
        veniceResponse.setVersion(version);
      }
    };
  }

  /**
   * Set backup version as current version.
   */
  public Route rollbackToBackupVersion(Admin admin) {
    return new VeniceRouteHandler<ControllerResponse>(ControllerResponse.class) {
      @Override
      public void internalHandle(Request request, ControllerResponse veniceResponse) {
        // Only allow allowlist users to run this command
        if (!checkIsAllowListUser(request, veniceResponse, () -> isAllowListUser(request))) {
          return;
        }
        AdminSparkServer.validateParams(request, ROLLBACK_TO_BACKUP_VERSION.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        admin.rollbackToBackupVersion(clusterName, storeName);

        veniceResponse.setCluster(clusterName);
        veniceResponse.setName(storeName);
      }
    };
  }

  public Route rollForwardToFutureVersion(Admin admin) {
    return new VeniceRouteHandler<ControllerResponse>(ControllerResponse.class) {
      @Override
      public void internalHandle(Request request, ControllerResponse veniceResponse) {
        // Only allow allowlist users to run this command
        if (!checkIsAllowListUser(request, veniceResponse, () -> isAllowListUser(request))) {
          return;
        }
        AdminSparkServer.validateParams(request, ROLL_FORWARD_TO_FUTURE_VERSION.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        admin.rollForwardToFutureVersion(clusterName, storeName);

        veniceResponse.setCluster(clusterName);
        veniceResponse.setName(storeName);
      }
    };
  }

  /**
   * enable/disable store read/write ability
   */
  public Route enableStore(Admin admin) {
    return new VeniceRouteHandler<ControllerResponse>(ControllerResponse.class) {
      @Override
      public void internalHandle(Request request, ControllerResponse veniceResponse) {
        // Only allow allowlist users to run this command
        if (!checkIsAllowListUser(request, veniceResponse, () -> isAllowListUser(request))) {
          return;
        }
        AdminSparkServer.validateParams(request, ENABLE_STORE.getParams(), admin);
        String cluster = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        String operation = request.queryParams(OPERATION);
        boolean status = Utils.parseBooleanFromString(request.queryParams(STATUS), "storeAccessStatus");

        veniceResponse.setCluster(cluster);
        veniceResponse.setName(storeName);

        if (operation.equals(READ_OPERATION)) {
          admin.setStoreReadability(cluster, storeName, status);
        } else if ((operation.equals(WRITE_OPERATION))) {
          admin.setStoreWriteability(cluster, storeName, status);
        } else if (operation.equals(READ_WRITE_OPERATION)) {
          admin.setStoreReadWriteability(cluster, storeName, status);
        } else {
          throw new VeniceException(OPERATION + " parameter:" + operation + " is invalid.");
        }
      }
    };
  }

  /**
   * @see Admin#deleteAllVersionsInStore(String, String)
   */
  public Route deleteAllVersions(Admin admin) {
    return new VeniceRouteHandler<MultiVersionResponse>(MultiVersionResponse.class) {
      @Override
      public void internalHandle(Request request, MultiVersionResponse veniceResponse) {
        // Only allow allowlist users to run this command
        if (!checkIsAllowListUser(request, veniceResponse, () -> isAllowListUser(request))) {
          return;
        }
        AdminSparkServer.validateParams(request, DELETE_ALL_VERSIONS.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        veniceResponse.setCluster(clusterName);
        veniceResponse.setName(storeName);
        List<Version> deletedVersions;
        Optional<AdminCommandExecutionTracker> adminCommandExecutionTracker =
            admin.getAdminCommandExecutionTracker(clusterName);
        if (adminCommandExecutionTracker.isPresent()) {
          // Lock the tracker to get the execution id for the last admin command.
          // If will not make our performance worse, because we lock the whole cluster while handling the admin
          // operation in parent admin.
          synchronized (adminCommandExecutionTracker) {
            deletedVersions = admin.deleteAllVersionsInStore(clusterName, storeName);
            veniceResponse.setExecutionId(adminCommandExecutionTracker.get().getLastExecutionId());
          }
        } else {
          deletedVersions = admin.deleteAllVersionsInStore(clusterName, storeName);
        }

        int[] deletedVersionNumbers = new int[deletedVersions.size()];
        for (int i = 0; i < deletedVersions.size(); i++) {
          deletedVersionNumbers[i] = deletedVersions.get(i).getNumber();
        }
        veniceResponse.setVersions(deletedVersionNumbers);
      }
    };
  }

  /**
   * @see Admin#deleteOldVersionInStore(String, String, int)
   */
  public Route deleteOldVersions(Admin admin) {
    return new VeniceRouteHandler<VersionResponse>(VersionResponse.class) {
      @Override
      public void internalHandle(Request request, VersionResponse veniceResponse) {
        // Only allow allowlist users to run this command
        if (!checkIsAllowListUser(request, veniceResponse, () -> isAllowListUser(request))) {
          return;
        }
        AdminSparkServer.validateParams(request, DELETE_ALL_VERSIONS.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        int versionNum = Integer.parseInt(request.queryParams(VERSION));
        veniceResponse.setCluster(clusterName);
        veniceResponse.setName(storeName);
        veniceResponse.setVersion(versionNum);
        admin.deleteOldVersionInStore(clusterName, storeName, versionNum);
      }
    };
  }

  /**
   * @see Admin#getStorageEngineOverheadRatio(String)
   */
  public Route getStorageEngineOverheadRatio(Admin admin) {
    return new VeniceRouteHandler<StorageEngineOverheadRatioResponse>(StorageEngineOverheadRatioResponse.class) {
      @Override
      public void internalHandle(Request request, StorageEngineOverheadRatioResponse veniceResponse) {
        // No ACL check for getting store metadata
        AdminSparkServer.validateParams(request, STORAGE_ENGINE_OVERHEAD_RATIO.getParams(), admin);

        veniceResponse.setCluster(request.queryParams(CLUSTER));
        veniceResponse.setName(request.queryParams(NAME));
        veniceResponse.setStorageEngineOverheadRatio(admin.getStorageEngineOverheadRatio(request.queryParams(CLUSTER)));
      }
    };
  }

  /**
   * @see Admin#configureNativeReplication(String, VeniceUserStoreType, Optional, boolean, Optional, Optional)
   */
  public Route enableNativeReplicationForCluster(Admin admin) {
    return new VeniceRouteHandler<ControllerResponse>(ControllerResponse.class) {
      @Override
      public void internalHandle(Request request, ControllerResponse veniceResponse) {
        // Only allow allowlist users to run this command
        if (!checkIsAllowListUser(request, veniceResponse, () -> isAllowListUser(request))) {
          return;
        }

        AdminSparkServer.validateParams(request, CONFIGURE_NATIVE_REPLICATION_FOR_CLUSTER.getParams(), admin);

        VeniceUserStoreType storeType = VeniceUserStoreType.valueOf(request.queryParams(STORE_TYPE).toUpperCase());

        String cluster = request.queryParams(CLUSTER);
        boolean enableNativeReplicationForCluster = Utils.parseBooleanFromString(request.queryParams(STATUS), STATUS);
        String sourceRegionParams = request.queryParamOrDefault(NATIVE_REPLICATION_SOURCE_FABRIC, null);
        String regionsFilterParams = request.queryParamOrDefault(REGIONS_FILTER, null);

        admin.configureNativeReplication(
            cluster,
            storeType,
            Optional.empty(),
            enableNativeReplicationForCluster,
            Optional.ofNullable(sourceRegionParams),
            Optional.ofNullable(regionsFilterParams));

        veniceResponse.setCluster(cluster);
      }
    };
  }

  /**
   * @see Admin#configureActiveActiveReplication(String, VeniceUserStoreType, Optional, boolean, Optional)
   */
  public Route enableActiveActiveReplicationForCluster(Admin admin) {
    return new VeniceRouteHandler<ControllerResponse>(ControllerResponse.class) {
      @Override
      public void internalHandle(Request request, ControllerResponse veniceResponse) {
        // Only allow allowlist users to run this command
        if (!checkIsAllowListUser(request, veniceResponse, () -> isAllowListUser(request))) {
          return;
        }

        AdminSparkServer.validateParams(request, CONFIGURE_ACTIVE_ACTIVE_REPLICATION_FOR_CLUSTER.getParams(), admin);

        VeniceUserStoreType storeType = VeniceUserStoreType.valueOf(request.queryParams(STORE_TYPE).toUpperCase());

        String cluster = request.queryParams(CLUSTER);
        boolean enableActiveActiveReplicationForCluster =
            Utils.parseBooleanFromString(request.queryParams(STATUS), STATUS);
        String regionsFilterParams = request.queryParamOrDefault(REGIONS_FILTER, null);

        admin.configureActiveActiveReplication(
            cluster,
            storeType,
            Optional.empty(),
            enableActiveActiveReplicationForCluster,
            Optional.ofNullable(regionsFilterParams));

        veniceResponse.setCluster(cluster);
      }
    };
  }

  /**
   * @see TopicManager#updateTopicCompactionPolicy(PubSubTopic, boolean)
   */
  public Route setTopicCompaction(Admin admin) {
    return new VeniceRouteHandler<StoreResponse>(StoreResponse.class) {
      @Override
      public void internalHandle(Request request, StoreResponse veniceResponse) {
        if (!isAllowListUser(request)) {
          veniceResponse.setError("Access Denied!! Only admins can change topic compaction policy!");
          veniceResponse.setErrorType(ErrorType.BAD_REQUEST);
          return;
        }
        AdminSparkServer.validateParams(request, SET_TOPIC_COMPACTION.getParams(), admin);
        try {
          admin.getTopicManager()
              .updateTopicCompactionPolicy(
                  pubSubTopicRepository.getTopic(request.queryParams(TOPIC)),
                  Boolean.getBoolean(request.queryParams(TOPIC_COMPACTION_POLICY)));
          veniceResponse.setName(request.queryParams(TOPIC));
        } catch (PubSubTopicDoesNotExistException e) {
          veniceResponse.setError("Topic does not exist!! Message: " + e.getMessage());
        }
      }
    };
  }

  /**
   * @see TopicCleanupService#extractVersionTopicsToCleanup(Admin, Map, int, int)
   */
  public Route getDeletableStoreTopics(Admin admin) {
    return new VeniceRouteHandler<MultiStoreTopicsResponse>(MultiStoreTopicsResponse.class) {
      @Override
      public void internalHandle(Request request, MultiStoreTopicsResponse veniceResponse) {
        AdminSparkServer.validateParams(request, GET_DELETABLE_STORE_TOPICS.getParams(), admin);
        try {
          Map<PubSubTopic, Long> allTopicRetentions = admin.getTopicManager().getAllTopicRetentions();
          Map<String, Map<PubSubTopic, Long>> allStoreTopics =
              TopicCleanupService.getAllVeniceStoreTopicsRetentions(allTopicRetentions);
          List<String> deletableTopicsList = new ArrayList<>();
          int minNumberOfUnusedKafkaTopicsToPreserve = admin.getMinNumberOfUnusedKafkaTopicsToPreserve();
          allStoreTopics.forEach((storeName, topicsWithRetention) -> {
            PubSubTopic realTimeTopic = pubSubTopicRepository.getTopic(Version.composeRealTimeTopic(storeName));
            if (topicsWithRetention.containsKey(realTimeTopic)) {
              if (admin.isTopicTruncatedBasedOnRetention(topicsWithRetention.get(realTimeTopic))) {
                deletableTopicsList.add(realTimeTopic.getName());
              }
              topicsWithRetention.remove(realTimeTopic);
            }
            List<PubSubTopic> deletableTopicsForThisStore = TopicCleanupService
                .extractVersionTopicsToCleanup(admin, topicsWithRetention, minNumberOfUnusedKafkaTopicsToPreserve, 0);
            if (!deletableTopicsForThisStore.isEmpty()) {
              deletableTopicsList
                  .addAll(deletableTopicsForThisStore.stream().map(PubSubTopic::getName).collect(Collectors.toList()));
            }
          });
          veniceResponse.setTopics(deletableTopicsList);
        } catch (Exception e) {
          veniceResponse.setError("Failed to list deletable store topics.", e);
        }
      }
    };
  }

  /**
   * @see Admin#compareStore(String, String, String, String)
   */
  public Route compareStore(Admin admin) {
    return new VeniceRouteHandler<StoreComparisonResponse>(StoreComparisonResponse.class) {
      @Override
      public void internalHandle(Request request, StoreComparisonResponse veniceResponse) {
        AdminSparkServer.validateParams(request, COMPARE_STORE.getParams(), admin);
        try {
          String clusterName = request.queryParams(CLUSTER);
          String storeName = request.queryParams(NAME);
          String fabricA = request.queryParams(FABRIC_A);
          String fabricB = request.queryParams(FABRIC_B);
          StoreComparisonInfo info = admin.compareStore(clusterName, storeName, fabricA, fabricB);
          veniceResponse.setCluster(clusterName);
          veniceResponse.setName(storeName);
          veniceResponse.setPropertyDiff(info.getPropertyDiff());
          veniceResponse.setSchemaDiff(info.getSchemaDiff());
          veniceResponse.setVersionStateDiff(info.getVersionStateDiff());
        } catch (Exception e) {
          veniceResponse.setError("Failed to compare store.", e);
        }
      }
    };
  }

  /**
   * @see Admin#getClusterStaleStores(String)
   */
  public Route getStaleStoresInCluster(Admin admin) {
    return new VeniceRouteHandler<ClusterStaleDataAuditResponse>(ClusterStaleDataAuditResponse.class) {
      @Override
      public void internalHandle(Request request, ClusterStaleDataAuditResponse veniceResponse) {
        AdminSparkServer.validateParams(request, GET_STALE_STORES_IN_CLUSTER.getParams(), admin);
        String cluster = request.queryParams(CLUSTER);
        Map<String, StoreDataAudit> staleStores = admin.getClusterStaleStores(cluster);
        veniceResponse.setAuditMap(staleStores);
        veniceResponse.setCluster(cluster);
      }
    };
  }

  /**
   * @see Admin#getClusterStores(String)
   */
  public Route getStoresInCluster(Admin admin) {
    return new VeniceRouteHandler<MultiStoreInfoResponse>(MultiStoreInfoResponse.class) {
      @Override
      public void internalHandle(Request request, MultiStoreInfoResponse veniceResponse) {
        AdminSparkServer.validateParams(request, GET_STORES_IN_CLUSTER.getParams(), admin);
        String cluster = request.queryParams(CLUSTER);
        ArrayList<StoreInfo> response = admin.getClusterStores(cluster);
        veniceResponse.setStoreInfoList(response);
        veniceResponse.setCluster(cluster);
      }
    };
  }

  /**
   * @see Admin#getLargestUsedVersionFromStoreGraveyard(String, String)
   */
  public Route getStoreLargestUsedVersion(Admin admin) {
    return new VeniceRouteHandler<VersionResponse>(VersionResponse.class) {
      @Override
      public void internalHandle(Request request, VersionResponse veniceResponse) {
        AdminSparkServer.validateParams(request, GET_STORES_IN_CLUSTER.getParams(), admin);
        String cluster = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        veniceResponse.setVersion(admin.getLargestUsedVersionFromStoreGraveyard(cluster, storeName));
      }
    };
  }

  /**
   * @see Admin#listStorePushInfo(String, String, boolean)
   */
  public Route listStorePushInfo(Admin admin) {
    return new VeniceRouteHandler<StoreHealthAuditResponse>(StoreHealthAuditResponse.class) {
      @Override
      public void internalHandle(Request request, StoreHealthAuditResponse veniceResponse) {
        AdminSparkServer.validateParams(request, LIST_STORE_PUSH_INFO.getParams(), admin);
        String cluster = request.queryParams(CLUSTER);
        String store = request.queryParams(NAME);
        boolean isPartitionDetailEnabled = Boolean.valueOf(request.queryParams(PARTITION_DETAIL_ENABLED));
        Map<String, RegionPushDetails> details = admin.listStorePushInfo(cluster, store, isPartitionDetailEnabled);

        veniceResponse.setName(store);
        veniceResponse.setCluster(cluster);
        veniceResponse.setRegionPushDetails(details);
      }
    };
  }

  /**
   * @see Admin#getRegionPushDetails(String, String, boolean)
   */
  public Route getRegionPushDetails(Admin admin) {
    return new VeniceRouteHandler<RegionPushDetailsResponse>(RegionPushDetailsResponse.class) {
      @Override
      public void internalHandle(Request request, RegionPushDetailsResponse veniceResponse) {
        AdminSparkServer.validateParams(request, GET_REGION_PUSH_DETAILS.getParams(), admin);
        String store = request.queryParams(NAME);
        String cluster = request.queryParams(CLUSTER);
        boolean isPartitionDetailEnabled = Boolean.valueOf(request.queryParams(PARTITION_DETAIL_ENABLED));
        RegionPushDetails details = admin.getRegionPushDetails(cluster, store, isPartitionDetailEnabled);
        veniceResponse.setRegionPushDetails(details);
      }
    };
  }

  /**
   * @see Admin#truncateKafkaTopic(String)
   */
  public Route deleteKafkaTopic(Admin admin) {
    return new VeniceRouteHandler<ControllerResponse>(ControllerResponse.class) {
      @Override
      public void internalHandle(Request request, ControllerResponse veniceResponse) {
        if (!checkIsAllowListUser(request, veniceResponse, () -> isAllowListUser(request))) {
          return;
        }
        AdminSparkServer.validateParams(request, DELETE_KAFKA_TOPIC.getParams(), admin);
        String cluster = request.queryParams(CLUSTER);
        String topicName = request.queryParams(TOPIC);
        admin.truncateKafkaTopic(topicName);
        veniceResponse.setCluster(cluster);
      }
    };
  }

  public Route removeStoreFromGraveyard(Admin admin) {
    return (request, response) -> {
      ControllerResponse responseObject = new ControllerResponse();
      response.type(HttpConstants.JSON);
      try {
        if (!isAllowListUser(request)) {
          response.status(HttpStatus.SC_FORBIDDEN);
          responseObject.setError(ACL_CHECK_FAILURE_WARN_MESSAGE_PREFIX + request.url());
          responseObject.setErrorType(ErrorType.BAD_REQUEST);
          return AdminSparkServer.OBJECT_MAPPER.writeValueAsString(responseObject);
        }
        AdminSparkServer.validateParams(request, REMOVE_STORE_FROM_GRAVEYARD.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        responseObject.setCluster(clusterName);
        responseObject.setName(storeName);
        admin.removeStoreFromGraveyard(clusterName, storeName);
      } catch (ResourceStillExistsException exception) {
        responseObject.setError(exception);
        AdminSparkServer.handleError(exception, request, response, false);
      } catch (Throwable throwable) {
        responseObject.setError(throwable);
        AdminSparkServer.handleError(new VeniceException(throwable), request, response);
      }
      return AdminSparkServer.OBJECT_MAPPER.writeValueAsString(responseObject);
    };
  }

  public Route sendHeartbeatToSystemStore(Admin admin) {
    return (request, response) -> {
      ControllerResponse responseObject = new ControllerResponse();
      response.type(HttpConstants.JSON);
      try {
        if (!isAllowListUser(request)) {
          response.status(HttpStatus.SC_FORBIDDEN);
          responseObject.setError(ACL_CHECK_FAILURE_WARN_MESSAGE_PREFIX + request.url());
          responseObject.setErrorType(ErrorType.BAD_REQUEST);
          return AdminSparkServer.OBJECT_MAPPER.writeValueAsString(responseObject);
        }
        AdminSparkServer.validateParams(request, SEND_HEARTBEAT_TIMESTAMP_TO_SYSTEM_STORE.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        long heartbeatTimestamp = Long.parseLong(request.queryParams(HEARTBEAT_TIMESTAMP));
        responseObject.setCluster(clusterName);
        responseObject.setName(storeName);
        admin.sendHeartbeatToSystemStore(clusterName, storeName, heartbeatTimestamp);
      } catch (Throwable e) {
        responseObject.setError(e);
      }
      return AdminSparkServer.OBJECT_MAPPER.writeValueAsString(responseObject);
    };
  }

  public Route getHeartbeatFromSystemStore(Admin admin) {
    return (request, response) -> {
      SystemStoreHeartbeatResponse responseObject = new SystemStoreHeartbeatResponse();
      response.type(HttpConstants.JSON);
      try {
        if (!isAllowListUser(request)) {
          response.status(HttpStatus.SC_FORBIDDEN);
          responseObject.setError(ACL_CHECK_FAILURE_WARN_MESSAGE_PREFIX + request.url());
          responseObject.setErrorType(ErrorType.BAD_REQUEST);
          return AdminSparkServer.OBJECT_MAPPER.writeValueAsString(responseObject);
        }
        AdminSparkServer.validateParams(request, GET_HEARTBEAT_TIMESTAMP_FROM_SYSTEM_STORE.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        responseObject.setCluster(clusterName);
        responseObject.setName(storeName);
        responseObject.setHeartbeatTimestamp(admin.getHeartbeatFromSystemStore(clusterName, storeName));
        responseObject.setHeartbeatTimestamp(0);
      } catch (Throwable e) {
        responseObject.setError(e);
      }
      return AdminSparkServer.OBJECT_MAPPER.writeValueAsString(responseObject);
    };
  }
}
