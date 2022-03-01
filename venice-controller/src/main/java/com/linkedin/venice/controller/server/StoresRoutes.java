package com.linkedin.venice.controller.server;

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
import com.linkedin.venice.controllerapi.RepushInfoResponse;
import com.linkedin.venice.controllerapi.RepushInfo;
import com.linkedin.venice.controllerapi.StorageEngineOverheadRatioResponse;
import com.linkedin.venice.controllerapi.StoreComparisonInfo;
import com.linkedin.venice.controllerapi.StoreComparisonResponse;
import com.linkedin.venice.controllerapi.StoreMigrationResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.TrackableControllerResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.kafka.TopicDoesNotExistException;
import com.linkedin.venice.meta.IncrementalPushPolicy;
import com.linkedin.venice.meta.StoreDataAudit;
import com.linkedin.venice.meta.VeniceUserStoreType;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.ZKStore;
import com.linkedin.venice.systemstore.schemas.StoreProperties;
import com.linkedin.venice.utils.Utils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import spark.Request;
import spark.Route;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.*;
import static com.linkedin.venice.controllerapi.ControllerRoute.*;


public class StoresRoutes extends AbstractRoute {
  public StoresRoutes(Optional<DynamicAccessController> accessController) {
    super(accessController);
  }

  /**
   * No ACL check; any user can try to list stores. If we get abused in future, we should only allow Venice admins
   * to run this command.
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
        boolean excludeSystemStores = (null != includeSystemStores && !Boolean.parseBoolean(includeSystemStores));
        Optional<String> storeConfigNameFilter = Optional.ofNullable(request.queryParamOrDefault(STORE_CONFIG_NAME_FILTER, null));
        Optional<String> storeConfigValueFilter = Optional.ofNullable(request.queryParamOrDefault(STORE_CONFIG_VALUE_FILTER, null));
        if (storeConfigNameFilter.isPresent()^storeConfigValueFilter.isPresent()) {
          throw new VeniceException("Missing parameter: " + (storeConfigNameFilter.isPresent() ? "store_config_value_filter" : "store_config_name_filter"));
        }
        boolean isDataReplicationPolicyConfigFilter = false;
        Schema.Field configFilterField = null;
        if (storeConfigNameFilter.isPresent()) {
          configFilterField = StoreProperties.SCHEMA$.getField(storeConfigNameFilter.get());
          if (configFilterField == null) {
            isDataReplicationPolicyConfigFilter = storeConfigNameFilter.get().equalsIgnoreCase("dataReplicationPolicy");
            if (!isDataReplicationPolicyConfigFilter) {
              throw new VeniceException("The config name filter " + storeConfigNameFilter.get() + " is not a valid store config.");
            }
          }
        }

        List<Store> storeList = admin.getAllStores(veniceResponse.getCluster());
        List<Store> selectedStoreList;
        if (excludeSystemStores || storeConfigNameFilter.isPresent()) {
          selectedStoreList = new ArrayList<>();
          for (Store store : storeList) {
            if (excludeSystemStores && store.isSystemStore()) {
              continue;
            }
            if (storeConfigValueFilter.isPresent()) {
              boolean configValueMatch = false;
              if (isDataReplicationPolicyConfigFilter) {
                if (!store.isHybrid() || store.getHybridStoreConfig().getDataReplicationPolicy() == null) {
                  continue;
                }
                configValueMatch = store.getHybridStoreConfig().getDataReplicationPolicy().name().equalsIgnoreCase(storeConfigValueFilter.get());
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
                        "Store config filtering for Schema type " + fieldSchema.getType().toString() + " is not supported");
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
        if (null == store) {
          throw new VeniceNoStoreException(storeName);
        }
        StoreInfo storeInfo = StoreInfo.fromStore(store);
        // Make sure store info will have right default retention time for Nuage UI display.
        if (storeInfo.getBackupVersionRetentionMs() < 0) {
          storeInfo.setBackupVersionRetentionMs(admin.getBackupVersionDefaultRetentionMs());
        }
        storeInfo.setColoToCurrentVersions(
            admin.getCurrentVersionsForMultiColos(clusterName, storeName));
        boolean isSSL = admin.isSSLEnabledForPush(clusterName, storeName);
        storeInfo.setKafkaBrokerUrl(admin.getKafkaBootstrapServers(isSSL));

        veniceResponse.setStore(storeInfo);
      }
    };
  }

  public Route getFutureVersion(Admin admin) {
    return new VeniceRouteHandler<MultiStoreStatusResponse>(MultiStoreStatusResponse.class) {
      @Override
      public void internalHandle(Request request, MultiStoreStatusResponse veniceResponse) {
        AdminSparkServer.validateParams(request, FUTURE_VERSION.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        veniceResponse.setCluster(clusterName);
        Map<String, String> storeStatusMap = admin.getFutureVersionsForMultiColos(clusterName, storeName);
        if(storeStatusMap.isEmpty()) {
          // Non parent controllers will return an empty map, so we'll just return the childs version of this api
          storeStatusMap = Collections.singletonMap(storeName, String.valueOf(admin.getFutureVersion(clusterName, storeName)));
        }
        veniceResponse.setStoreStatusMap(storeStatusMap);
      }
    };
  }

  public Route migrateStore(Admin admin) {
    return new VeniceRouteHandler<StoreMigrationResponse>(StoreMigrationResponse.class) {
      @Override
      public void internalHandle(Request request, StoreMigrationResponse veniceResponse) {
        // Only allow whitelist users to run this command
        if (!isAllowListUser(request)) {
          veniceResponse.setError("Only admin users are allowed to run " + request.url());
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
          veniceResponse.setError("Store " + storeName + " belongs to cluster " + clusterDiscovered
              + ", which is different from the given src cluster name " + srcClusterName);
          return;
        }
        // Store should not belong to dest cluster already
        if (clusterDiscovered.equals(destClusterName)) {
          veniceResponse.setError("Store " + storeName + " already belongs to cluster " + destClusterName);
          return;
        }

        admin.migrateStore(srcClusterName, destClusterName, storeName);
      }
    };
  }

  public Route completeMigration(Admin admin) {
    return new VeniceRouteHandler<StoreMigrationResponse>(StoreMigrationResponse.class) {
      @Override
      public void internalHandle(Request request, StoreMigrationResponse veniceResponse) {
        // Only allow whitelist users to run this command
        if (!isAllowListUser(request)) {
          veniceResponse.setError("Only admin users are allowed to run " + request.url());
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
          veniceResponse.setError("Store " + storeName + " belongs to cluster " + clusterDiscovered
              + ", which is different from the given src cluster name " + srcClusterName);
          return;
        }
        // Store should not belong to dest cluster already
        if (clusterDiscovered.equals(destClusterName)) {
          veniceResponse.setError("Store " + storeName + " already belongs to cluster " + destClusterName);
          return;
        }

        admin.completeMigration(srcClusterName, destClusterName, storeName);
      }
    };
  }

  public Route abortMigration(Admin admin) {
    return new VeniceRouteHandler<StoreMigrationResponse>(StoreMigrationResponse.class) {
      @Override
      public void internalHandle(Request request, StoreMigrationResponse veniceResponse) {
        try {
          // Only allow whitelist users to run this command
          if (!isAllowListUser(request)) {
            veniceResponse.setError("Only admin users are allowed to run " + request.url());
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
          veniceResponse.setError(e.getMessage());
        }
      }
    };
  }

  public Route deleteStore(Admin admin) {
    return new VeniceRouteHandler<TrackableControllerResponse>(TrackableControllerResponse.class) {
      @Override
      public void internalHandle(Request request, TrackableControllerResponse veniceResponse) {
        // Only allow whitelist users to run this command
        if (!isAllowListUser(request)) {
          veniceResponse.setError("Only admin users are allowed to run " + request.url());
          return;
        }
        AdminSparkServer.validateParams(request, DELETE_STORE.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);

        veniceResponse.setCluster(clusterName);
        veniceResponse.setName(storeName);

        Optional<AdminCommandExecutionTracker> adminCommandExecutionTracker = admin.getAdminCommandExecutionTracker(clusterName);
        if (adminCommandExecutionTracker.isPresent()) {
          // Lock the tracker to get the execution id for the last admin command.
          // If will not make our performance worse, because we lock the whole cluster while handling the admin operation in parent admin.
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

  public Route updateStore(Admin admin) {
    return new VeniceRouteHandler<ControllerResponse>(ControllerResponse.class) {
      @Override
      public void internalHandle(Request request, ControllerResponse veniceResponse) {
        // Only allow whitelist users to run this command
        if (!isAllowListUser(request)) {
          veniceResponse.setError("Only admin users are allowed to run " + request.url());
          return;
        }
        AdminSparkServer.validateParams(request, UPDATE_STORE.getParams(), admin);
        //TODO: we may want to have a specific response for store updating
        String clusterName = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);

        veniceResponse.setCluster(clusterName);
        veniceResponse.setName(storeName);

        Map<String, String> params = Utils.extractQueryParamsFromRequest(request.queryMap().toMap(), veniceResponse);

        try {
          admin.updateStore(clusterName, storeName, new UpdateStoreQueryParams(params));
        } catch (Exception e) {
          veniceResponse.setError("Failed when updating store " + storeName + ". Exception type: " + e.getClass().toString() + ". Detailed message = " + e.getMessage());
        }
      }
    };
  }

  public Route setOwner(Admin admin) {
    return new VeniceRouteHandler<OwnerResponse>(OwnerResponse.class) {
      @Override
      public void internalHandle(Request request, OwnerResponse veniceResponse) {
        // Only admin users are allowed to update owners; regular user can do it through Nuage
        if (!isAllowListUser(request)) {
          veniceResponse.setError("ACL failed for request " + request.url());
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
      }
    };
  }

  public Route setCurrentVersion(Admin admin) {
    return new VeniceRouteHandler<VersionResponse>(VersionResponse.class) {
      @Override
      public void internalHandle(Request request, VersionResponse veniceResponse) {
        // Only allow whitelist users to run this command
        if (!isAllowListUser(request)) {
          veniceResponse.setError("Only admin users are allowed to run " + request.url());
          return;
        }
        AdminSparkServer.validateParams(request, SET_VERSION.getParams(), admin); //throws venice exception
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
   * enable/disable store read/write ability
   */
  public Route enableStore(Admin admin) {
    return new VeniceRouteHandler<ControllerResponse>(ControllerResponse.class) {
      @Override
      public void internalHandle(Request request, ControllerResponse veniceResponse) {
        // Only allow whitelist users to run this command
        if (!isAllowListUser(request)) {
          veniceResponse.setError("Only admin users are allowed to run " + request.url());
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

  public Route deleteAllVersions(Admin admin) {
    return new VeniceRouteHandler<MultiVersionResponse>(MultiVersionResponse.class) {
      @Override
      public void internalHandle(Request request, MultiVersionResponse veniceResponse) {
        // Only allow whitelist users to run this command
        if (!isAllowListUser(request)) {
          veniceResponse.setError("Only admin users are allowed to run " + request.url());
          return;
        }
        AdminSparkServer.validateParams(request, DELETE_ALL_VERSIONS.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        veniceResponse.setCluster(clusterName);
        veniceResponse.setName(storeName);
        List<Version> deletedVersions = Collections.emptyList();
        Optional<AdminCommandExecutionTracker> adminCommandExecutionTracker = admin.getAdminCommandExecutionTracker(clusterName);
        if (adminCommandExecutionTracker.isPresent()) {
          // Lock the tracker to get the execution id for the last admin command.
          // If will not make our performance worse, because we lock the whole cluster while handling the admin operation in parent admin.
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

  public Route deleteOldVersions(Admin admin) {
    return new VeniceRouteHandler<VersionResponse>(VersionResponse.class) {
      @Override
      public void internalHandle(Request request, VersionResponse veniceResponse) {
        // Only allow whitelist users to run this command
        if (!isAllowListUser(request)) {
          veniceResponse.setError("Only admin users are allowed to run " + request.url());
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

  /*
   * No ACL check; any user can try to list stores.
   */
  public Route getLFModelStores(Admin admin) {
    return new VeniceRouteHandler<MultiStoreResponse>(MultiStoreResponse.class) {
      @Override
      public void internalHandle(Request request, MultiStoreResponse veniceResponse) {
        AdminSparkServer.validateParams(request, LIST_LF_STORES.getParams(), admin);
        veniceResponse.setCluster(request.queryParams(CLUSTER));
        veniceResponse.setName(request.queryParams(NAME));

        List<String> lFEnabledStores = admin.getAllStores(veniceResponse.getCluster()).stream()
            // Skip all the system stores
            .filter(store -> (store.isLeaderFollowerModelEnabled() && !store.isSystemStore()))
            .map(Store::getName)
            .collect(Collectors.toList());

        veniceResponse.setStores(lFEnabledStores.toArray(new String[lFEnabledStores.size()]));

      }
    };
  }

  public Route enableLFModelForStores(Admin admin) {
    return new VeniceRouteHandler<MultiStoreResponse>(MultiStoreResponse.class) {
      @Override
      public void internalHandle(Request request, MultiStoreResponse veniceResponse) {
        // Only allow whitelist users to run this command
        if (!isAllowListUser(request)) {
          veniceResponse.setError("Only admin users are allowed to run " + request.url());
          return;
        }

        AdminSparkServer.validateParams(request, ENABLE_LF_MODEL.getParams(), admin);
        veniceResponse.setName(request.queryParams(NAME));

        String storeType = request.queryParams(STORE_TYPE);

        String cluster = request.queryParams(CLUSTER);

        List<Store> storeCandidates;
        VeniceUserStoreType userStoreType = VeniceUserStoreType.valueOf(storeType.toUpperCase());
        switch (userStoreType) {
          case BATCH_ONLY:
            storeCandidates = admin.getAllStores(cluster).stream()
                .filter(store -> (!store.isHybrid() && !store.isIncrementalPushEnabled()))
                .collect(Collectors.toList());
            break;
          case HYBRID_ONLY:
            storeCandidates = admin.getAllStores(cluster).stream()
                .filter(store -> (store.isHybrid() && !store.isIncrementalPushEnabled()))
                .collect(Collectors.toList());
            break;
          case INCREMENTAL_PUSH:
            storeCandidates = admin.getAllStores(cluster).stream()
                .filter(Store::isIncrementalPushEnabled)
                .collect(Collectors.toList());
            break;
          case HYBRID_OR_INCREMENTAL:
            storeCandidates = admin.getAllStores(cluster).stream()
                .filter(store -> (store.isHybrid() || store.isIncrementalPushEnabled()))
                .collect(Collectors.toList());
            break;
          case ALL:
            storeCandidates = admin.getAllStores(cluster);
            break;
          default:
            throw new VeniceException("Unsupported store type." + storeType);
        }

        // filter out all the system stores
        storeCandidates = storeCandidates.stream().filter(store -> !store.isSystemStore()).collect(Collectors.toList());

        boolean isLFEnabled = Utils.parseBooleanFromString(request.queryParams(STATUS), "isLFEnabled");
        storeCandidates.forEach(store -> admin.enableLeaderFollowerModelLocally(cluster, store.getName(), isLFEnabled));

        veniceResponse.setCluster(cluster);

        veniceResponse.setStores(storeCandidates.stream().map(Store::getName).toArray(String[]::new));
      }
    };
  }

  public Route enableNativeReplicationForCluster(Admin admin) {
    return new VeniceRouteHandler<ControllerResponse>(ControllerResponse.class) {
      @Override
      public void internalHandle(Request request, ControllerResponse veniceResponse) {
        // Only allow whitelist users to run this command
        if (!isAllowListUser(request)) {
          veniceResponse.setError("Only admin users are allowed to run " + request.url());
          return;
        }

        AdminSparkServer.validateParams(request, CONFIGURE_NATIVE_REPLICATION_FOR_CLUSTER.getParams(), admin);

        VeniceUserStoreType storeType = VeniceUserStoreType.valueOf(request.queryParams(STORE_TYPE).toUpperCase());

        String cluster = request.queryParams(CLUSTER);
        boolean enableNativeReplicationForCluster = Utils.parseBooleanFromString(request.queryParams(STATUS), STATUS);
        String sourceRegionParams = request.queryParamOrDefault(NATIVE_REPLICATION_SOURCE_FABRIC, null);
        String regionsFilterParams = request.queryParamOrDefault(REGIONS_FILTER, null);

        admin.configureNativeReplication(cluster,
                                         storeType,
                                         Optional.empty(),
                                         enableNativeReplicationForCluster,
                                         (null == sourceRegionParams) ? Optional.empty() : Optional.of(sourceRegionParams),
                                         (null == regionsFilterParams) ? Optional.empty() : Optional.of(regionsFilterParams));

        veniceResponse.setCluster(cluster);
      }
    };
  }

  public Route enableActiveActiveReplicationForCluster(Admin admin) {
    return new VeniceRouteHandler<ControllerResponse>(ControllerResponse.class) {
      @Override
      public void internalHandle(Request request, ControllerResponse veniceResponse) {
        // Only allow whitelist users to run this command
        if (!isAllowListUser(request)) {
          veniceResponse.setError("Only admin users are allowed to run " + request.url());
          return;
        }

        AdminSparkServer.validateParams(request, CONFIGURE_ACTIVE_ACTIVE_REPLICATION_FOR_CLUSTER.getParams(), admin);

        VeniceUserStoreType storeType = VeniceUserStoreType.valueOf(request.queryParams(STORE_TYPE).toUpperCase());

        String cluster = request.queryParams(CLUSTER);
        boolean enableActiveActiveReplicationForCluster = Utils.parseBooleanFromString(request.queryParams(STATUS), STATUS);
        String regionsFilterParams = request.queryParamOrDefault(REGIONS_FILTER, null);

        admin.configureActiveActiveReplication(cluster,
            storeType,
            Optional.empty(),
            enableActiveActiveReplicationForCluster,
            (null == regionsFilterParams) ? Optional.empty() : Optional.of(regionsFilterParams));

        veniceResponse.setCluster(cluster);
      }
    };
  }

  public Route configureIncrementalPushForCluster(Admin admin) {
    return new VeniceRouteHandler<ControllerResponse>(ControllerResponse.class) {
      @Override
      public void internalHandle(Request request, ControllerResponse veniceResponse) {
        // Only allow whitelist users to run this command
        if (!isAllowListUser(request)) {
          veniceResponse.setError("Only admin users are allowed to run " + request.url());
          return;
        }

        AdminSparkServer.validateParams(request, CONFIGURE_INCREMENTAL_PUSH_FOR_CLUSTER.getParams(), admin);

        String cluster = request.queryParams(CLUSTER);
        IncrementalPushPolicy incrementalPushPolicyToApply = IncrementalPushPolicy.valueOf(request.queryParams(INCREMENTAL_PUSH_POLICY));
        Optional<IncrementalPushPolicy> incrementalPushPolicyToFilter = request.queryParamOrDefault(INCREMENTAL_PUSH_POLICY_TO_FILTER, null) != null
                                                                        ? Optional.of(IncrementalPushPolicy.valueOf(request.queryParams(INCREMENTAL_PUSH_POLICY_TO_FILTER)))
                                                                        : Optional.empty();
        String regionsFilterParams = request.queryParamOrDefault(REGIONS_FILTER, null);

        admin.configureIncrementalPushForCluster(cluster,
                                                 Optional.empty(),
                                                 incrementalPushPolicyToApply,
                                                 incrementalPushPolicyToFilter,
                                                 (null == regionsFilterParams) ? Optional.empty() : Optional.of(regionsFilterParams));

        veniceResponse.setCluster(cluster);
      }
    };
  }

  public Route setTopicCompaction(Admin admin) {
    return new VeniceRouteHandler<StoreResponse>(StoreResponse.class) {
      @Override
      public void internalHandle(Request request, StoreResponse veniceResponse) {
        if(!isAllowListUser(request)) {
          veniceResponse.setError("Access Denied!! Only admins can change topic compaction policy!");
          return;
        }
        AdminSparkServer.validateParams(request, SET_TOPIC_COMPACTION.getParams(), admin);
        try {
          admin.getTopicManager().updateTopicCompactionPolicy(request.queryParams(TOPIC), Boolean.getBoolean(request.queryParams(TOPIC_COMPACTION_POLICY)));
          veniceResponse.setName(request.queryParams(TOPIC));
        } catch (TopicDoesNotExistException e){
          veniceResponse.setError("Topic does not exist!! Message: " + e.getMessage());
        }
      }
    };
  }

  public Route getDeletableStoreTopics(Admin admin) {
    return new VeniceRouteHandler<MultiStoreTopicsResponse>(MultiStoreTopicsResponse.class) {
      @Override
      public void internalHandle(Request request, MultiStoreTopicsResponse veniceResponse) {
        AdminSparkServer.validateParams(request, GET_DELETABLE_STORE_TOPICS.getParams(), admin);
        try {
          Map<String, Map<String, Long>> allStoreTopics = TopicCleanupService.getAllVeniceStoreTopicsRetentions(admin.getTopicManager());
          List<String> deletableTopicsList = new ArrayList<>();
          int minNumberOfUnusedKafkaTopicsToPreserve = admin.getMinNumberOfUnusedKafkaTopicsToPreserve();
          allStoreTopics.forEach((storeName, topicsWithRetention) -> {
            String realTimeTopic = Version.composeRealTimeTopic(storeName);
            if (topicsWithRetention.containsKey(realTimeTopic)) {
              if (admin.isTopicTruncatedBasedOnRetention(topicsWithRetention.get(realTimeTopic))) {
                deletableTopicsList.add(realTimeTopic);
              }
              topicsWithRetention.remove(realTimeTopic);
            }
            List<String> deletableTopicsForThisStore = TopicCleanupService.extractVersionTopicsToCleanup(admin, topicsWithRetention,
                minNumberOfUnusedKafkaTopicsToPreserve, false);
            if (!deletableTopicsForThisStore.isEmpty()) {
              deletableTopicsList.addAll(deletableTopicsForThisStore);
            }
          });
          veniceResponse.setTopics(deletableTopicsList);
        } catch (Exception e){
          veniceResponse.setError("Failed to list deletable store topics. Message: " + e.getMessage());
        }
      }
    };
  }

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
          veniceResponse.setError("Failed to compare store. Message: " + e.getMessage());
        }
      }
    };
  }
  public Route getStaleStoresInCluster(Admin admin) {
    return new VeniceRouteHandler<ClusterStaleDataAuditResponse>(ClusterStaleDataAuditResponse.class) {
      @Override
      public void internalHandle(Request request, ClusterStaleDataAuditResponse veniceResponse) {
        AdminSparkServer.validateParams(request, GET_STALE_STORES_IN_CLUSTER.getParams(), admin);
        String cluster = request.queryParams(CLUSTER);
        Map<String, StoreDataAudit> staleStores = admin.getClusterStaleStores(cluster, Optional.empty());
        veniceResponse.setAuditMap(staleStores);
        veniceResponse.setCluster(cluster);
      }
    };
  }

  public Route getStoresInCluster(Admin admin) {
    return new VeniceRouteHandler<MultiStoreInfoResponse>(MultiStoreInfoResponse.class) {
      @Override
      public void internalHandle(Request request, MultiStoreInfoResponse veniceResponse) {
        AdminSparkServer.validateParams(request, GET_STORES_IN_CLUSTER.getParams(), admin);
        String cluster = request.queryParams(CLUSTER);
        List<StoreInfo> response = admin.getClusterStores(cluster);
        veniceResponse.setStoreInfoList(response);
        veniceResponse.setCluster(cluster);
      }
    };
  }

  public Route getStoreLargestUsedVersion(Admin admin) {
    return new VeniceRouteHandler<VersionResponse>(VersionResponse.class) {
      @Override
      public void internalHandle(Request request, VersionResponse veniceResponse) {
        //AdminSparkServer.validateParams(request, GET_STORES_IN_CLUSTER.getParams(), admin);
        String cluster = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        veniceResponse.setVersion(admin.getStoreLargestUsedVersion(cluster, storeName));
      }
    };
  }
}
