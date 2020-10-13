package com.linkedin.venice.controller.server;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.AdminCommandExecutionTracker;
import com.linkedin.venice.controller.VeniceParentHelixAdmin;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.MultiStoreResponse;
import com.linkedin.venice.controllerapi.MultiStoreStatusResponse;
import com.linkedin.venice.controllerapi.MultiVersionResponse;
import com.linkedin.venice.controllerapi.OwnerResponse;
import com.linkedin.venice.controllerapi.PartitionResponse;
import com.linkedin.venice.controllerapi.StorageEngineOverheadRatioResponse;
import com.linkedin.venice.controllerapi.StoreMigrationResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.TrackableControllerResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.kafka.TopicDoesNotExistException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.Utils;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import spark.Request;
import spark.Route;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.*;
import static com.linkedin.venice.controllerapi.ControllerRoute.*;
import static com.linkedin.venice.meta.LeaderFollowerEnabled.*;


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
        List<Store> storeList = admin.getAllStores(veniceResponse.getCluster());
        if (null != includeSystemStores // If the param is not provided, the default is to include them
            && !Boolean.parseBoolean(includeSystemStores)) {
          storeList = storeList.stream()
              .filter(store -> !store.isSystemStore())
              .collect(Collectors.toList());
        }

        String[] storeNameList = new String[storeList.size()];
        for (int i = 0; i < storeList.size(); i++) {
          storeNameList[i] = storeList.get(i).getName();
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

  public Route getStore(Admin admin) {
    return new VeniceRouteHandler<StoreResponse>(StoreResponse.class) {
      @Override
      public void internalHandle(Request request, StoreResponse veniceResponse) {
        // No ACL check for getting store metadata
        AdminSparkServer.validateParams(request, STORE.getParams(), admin);
        veniceResponse.setCluster(request.queryParams(CLUSTER));
        veniceResponse.setName(request.queryParams(NAME));
        Store store = admin.getStore(veniceResponse.getCluster(), veniceResponse.getName());
        if (null == store) {
          throw new VeniceNoStoreException(veniceResponse.getName());
        }
        StoreInfo storeInfo = StoreInfo.fromStore(store);
        storeInfo.setColoToCurrentVersions(
            admin.getCurrentVersionsForMultiColos(veniceResponse.getCluster(), veniceResponse.getName()));
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
        if (!isWhitelistUsers(request)) {
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

        // return child controller(s) url to admin-tool monitor if this is parent controller
        if (admin.getClass().isAssignableFrom(VeniceParentHelixAdmin.class)) {
          Map<String, String> childClusterMap = ((VeniceParentHelixAdmin) admin).getChildClusterMap(destClusterName);
          veniceResponse.setChildClusterMap(childClusterMap);
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
        if (!isWhitelistUsers(request)) {
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
          if (!isWhitelistUsers(request)) {
            veniceResponse.setError("Only admin users are allowed to run " + request.url());
            return;
          }
          AdminSparkServer.validateParams(request, ABORT_MIGRATION.getParams(), admin);
          String srcClusterName = request.queryParams(CLUSTER);
          String destClusterName = request.queryParams(CLUSTER_DEST);
          String storeName = request.queryParams(NAME);

          veniceResponse.setName(storeName);

          // return child controller(s) url to admin-tool monitor if this is parent controller
          if (admin.getClass().isAssignableFrom(VeniceParentHelixAdmin.class)) {
            Map<String, String> childClusterMap = ((VeniceParentHelixAdmin) admin).getChildClusterMap(destClusterName);
            veniceResponse.setChildClusterMap(childClusterMap);
          }

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
        if (!isWhitelistUsers(request)) {
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
            admin.deleteStore(clusterName, storeName, Store.IGNORE_VERSION);
            veniceResponse.setExecutionId(adminCommandExecutionTracker.get().getLastExecutionId());
          }
        } else {
          admin.deleteStore(clusterName, storeName, Store.IGNORE_VERSION);
        }
      }
    };
  }

  public Route updateStore(Admin admin) {
    return new VeniceRouteHandler<ControllerResponse>(ControllerResponse.class) {
      @Override
      public void internalHandle(Request request, ControllerResponse veniceResponse) {
        // Only allow whitelist users to run this command
        if (!isWhitelistUsers(request)) {
          veniceResponse.setError("Only admin users are allowed to run " + request.url());
          return;
        }
        AdminSparkServer.validateParams(request, UPDATE_STORE.getParams(), admin);
        //TODO: we may want to have a specific response for store updating
        String clusterName = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        veniceResponse.setCluster(clusterName);
        veniceResponse.setName(storeName);

        Map<String, String[]> sparkRequestParams = request.queryMap().toMap();

        boolean anyParamContainsMoreThanOneValue = sparkRequestParams.values().stream()
            .anyMatch(strings -> strings.length > 1);

        if (anyParamContainsMoreThanOneValue) {
          String errMsg =
              "Array parameters are not supported. Provided request parameters: " + sparkRequestParams.toString();
          veniceResponse.setError(errMsg);
          throw new VeniceException(errMsg);
        }

        Map<String, String> params = sparkRequestParams.entrySet().stream()
            // Extract the first (and only) value of each param
            .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()[0]));

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
        if (!isWhitelistUsers(request)) {
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
        if (!isWhitelistUsers(request)) {
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
        if (!isWhitelistUsers(request)) {
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
        if (!isWhitelistUsers(request)) {
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
        if (!isWhitelistUsers(request)) {
          veniceResponse.setError("Only admin users are allowed to run " + request.url());
          return;
        }
        AdminSparkServer.validateParams(request, DELETE_ALL_VERSIONS.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        int versionNum = Integer.valueOf(request.queryParams(VERSION));
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
            .filter(store -> store.isLeaderFollowerModelEnabled())
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
        if (!isWhitelistUsers(request)) {
          veniceResponse.setError("Only admin users are allowed to run " + request.url());
          return;
        }

        AdminSparkServer.validateParams(request, ENABLE_LF_MODEL.getParams(), admin);
        veniceResponse.setName(request.queryParams(NAME));

        String storeType = request.queryParams(STORE_TYPE);

        String cluster = request.queryParams(CLUSTER);

        List<Store> storeCandidates;
        if (storeType.equals(HYBRID_ONLY.toString())) {
          storeCandidates = admin.getAllStores(cluster).stream()
              .filter(Store::isHybrid)
              .collect(Collectors.toList());
        } else if (storeType.equals(HYBRID_OR_INCREMENTAL.toString())) {
          storeCandidates = admin.getAllStores(cluster).stream()
              .filter(store -> (store.isHybrid() || store.isIncrementalPushEnabled()))
              .collect(Collectors.toList());
        } else if (storeType.equals(ALL.toString())){
          storeCandidates = admin.getAllStores(cluster);
        } else {
          throw new VeniceException("Unsupported store type." + storeType);
        }

        boolean isLFEnabled = Utils.parseBooleanFromString(request.queryParams(STATUS), "isLFEnabled");
        storeCandidates.forEach(store -> admin.setLeaderFollowerModelEnabled(cluster, store.getName(), isLFEnabled));

        veniceResponse.setCluster(cluster);
        veniceResponse.setStores((String []) storeCandidates.toArray());
      }
    };
  }

  public Route dematerializeMetadataStoreVersion(Admin admin) {
    return (request, response) -> {
      ControllerResponse responseObject = new ControllerResponse();
      response.type(HttpConstants.JSON);
      try {
        AdminSparkServer.validateParams(request, DEMATERIALIZE_METADATA_STORE_VERSION.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        int versionNumber = Utils.parseIntFromString(request.queryParams(VERSION), VERSION);

        admin.dematerializeMetadataStoreVersion(clusterName, storeName, versionNumber, true);

        responseObject.setCluster(clusterName);
        responseObject.setName(storeName);
      } catch (Throwable e) {
        responseObject.setError(e.getMessage());
        AdminSparkServer.handleError(e, request, response);
      }
      return AdminSparkServer.mapper.writeValueAsString(responseObject);
    };
  }

  public Route setTopicCompaction(Admin admin) {
    return new VeniceRouteHandler<StoreResponse>(StoreResponse.class) {
      @Override
      public void internalHandle(Request request, StoreResponse veniceResponse) {
        if(!isWhitelistUsers(request)) {
          veniceResponse.setError("Access Denied!! Only admins can change topic compaction policy!");
          return;
        }
        AdminSparkServer.validateParams(request, SET_TOPIC_COMPACTION.getParams(), admin);
        //String clusterName = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        try {
          admin.getTopicManager().updateTopicCompactionPolicy(request.queryParams(TOPIC), Boolean.getBoolean(request.queryParams(TOPIC_COMPACTION_POLICY)));
          veniceResponse.setName(request.queryParams(TOPIC));
        } catch (TopicDoesNotExistException e){
          veniceResponse.setError("Topic does not exist!! Message: " + e.getMessage());
        }
      }
    };
  }
}
