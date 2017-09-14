package com.linkedin.venice.controller.server;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.AdminCommandExecutionTracker;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.MultiStoreResponse;
import com.linkedin.venice.controllerapi.MultiStoreStatusResponse;
import com.linkedin.venice.controllerapi.MultiVersionResponse;
import com.linkedin.venice.controllerapi.OwnerResponse;
import com.linkedin.venice.controllerapi.PartitionResponse;
import com.linkedin.venice.controllerapi.StorageEngineOverheadRatioResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.TrackableControllerResponse;
import com.linkedin.venice.controllerapi.VersionResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.Utils;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import spark.Request;
import spark.Route;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.*;
import static com.linkedin.venice.controllerapi.ControllerRoute.*;


public class StoresRoutes {
  public static Route getAllStores(Admin admin) {
    return new VeniceRouteHandler<MultiStoreResponse>(MultiStoreResponse.class) {
      @Override
      public void internalHandle(Request request, MultiStoreResponse veniceResponse) {
        AdminSparkServer.validateParams(request, LIST_STORES.getParams(), admin);
        veniceResponse.setCluster(request.queryParams(CLUSTER));
        veniceResponse.setName(request.queryParams(NAME));
        List<Store> storeList = admin.getAllStores(veniceResponse.getCluster());
        String[] storeNameList = new String[storeList.size()];
        for (int i = 0; i < storeList.size(); i++) {
          storeNameList[i] = storeList.get(i).getName();
        }
        veniceResponse.setStores(storeNameList);
      }
    };
  }

  public static Route getAllStoresStatuses(Admin admin) {
    return new VeniceRouteHandler<MultiStoreStatusResponse>(MultiStoreStatusResponse.class) {
      @Override
      public void internalHandle(Request request, MultiStoreStatusResponse veniceResponse) {
        AdminSparkServer.validateParams(request, CLUSTER_HELATH_STORES.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);
        veniceResponse.setCluster(clusterName);
        Map<String, String> storeStatusMap = admin.getAllStoreStatuses(clusterName);
        veniceResponse.setStoreStatusMap(storeStatusMap);
      }
    };
  }

  public static Route getStore(Admin admin) {
    return new VeniceRouteHandler<StoreResponse>(StoreResponse.class) {

      @Override
      public void internalHandle(Request request, StoreResponse veniceResponse) {
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

  public static Route deleteStore(Admin admin) {
    return new VeniceRouteHandler<TrackableControllerResponse>(TrackableControllerResponse.class) {
      @Override
      public void internalHandle(Request request, TrackableControllerResponse veniceResponse) {
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

  public static Route updateStore(Admin admin) {
    return new VeniceRouteHandler<ControllerResponse>(ControllerResponse.class) {

      @Override
      public void internalHandle(Request request, ControllerResponse veniceResponse) {
        AdminSparkServer.validateParams(request, UPDATE_STORE.getParams(), admin);
        //TODO: we may want to have a specific response for store updating
        veniceResponse.setCluster(request.queryParams(CLUSTER));
        veniceResponse.setName(request.queryParams(NAME));

        Optional<String> owner = Optional.ofNullable(AdminSparkServer.getOptionalParameterValue(request, OWNER));

        String currentVersionStr = AdminSparkServer.getOptionalParameterValue(request, VERSION);
        Optional<Integer> currentVersion = currentVersionStr == null ? Optional.empty() :
            Optional.of(Utils.parseIntFromString(currentVersionStr, "currentVersion"));

        String partitionCountStr = AdminSparkServer.getOptionalParameterValue(request, PARTITION_COUNT);
        Optional<Integer> partitionCount = partitionCountStr == null ? Optional.empty() :
            Optional.of(Utils.parseIntFromString(partitionCountStr, "partitionCount"));

        String readabilityStr = AdminSparkServer.getOptionalParameterValue(request, ENABLE_READS);
        Optional<Boolean> readability = readabilityStr == null ? Optional.empty() :
            Optional.of(Utils.parseBooleanFromString(readabilityStr, "enableReads"));

        String writeabilityStr = AdminSparkServer.getOptionalParameterValue(request, ENABLE_WRITES);
        Optional<Boolean> writeability = writeabilityStr == null ? Optional.empty() :
            Optional.of(Utils.parseBooleanFromString(writeabilityStr, "enableWrites"));

        String storageQuotaStr = AdminSparkServer.getOptionalParameterValue(request, STORAGE_QUOTA_IN_BYTE);
        Optional<Long> storageQuotaInByte = storageQuotaStr == null ? Optional.empty() :
            Optional.of(Utils.parseLongFromString(storageQuotaStr, "storageQuotaInByte"));

        String readQuotaStr = AdminSparkServer.getOptionalParameterValue(request, READ_QUOTA_IN_CU);
        Optional<Long> readQuotaInCU = readQuotaStr == null ? Optional.empty() :
            Optional.of(Utils.parseLongFromString(readQuotaStr, "readQuotaInCU"));

        String hybridRewindTimeStr = AdminSparkServer.getOptionalParameterValue(request, REWIND_TIME_IN_SECONDS);
        Optional<Long> hybridRewind = (null == hybridRewindTimeStr)
            ? Optional.empty()
            : Optional.of(Utils.parseLongFromString(hybridRewindTimeStr, REWIND_TIME_IN_SECONDS));

        String hybridOffsetLagStr = AdminSparkServer.getOptionalParameterValue(request, OFFSET_LAG_TO_GO_ONLINE);
        Optional<Long> hybridOffsetLag = (null == hybridOffsetLagStr)
            ? Optional.empty()
            : Optional.of(Utils.parseLongFromString(hybridOffsetLagStr, OFFSET_LAG_TO_GO_ONLINE));

        admin.updateStore(veniceResponse.getCluster(),
                          veniceResponse.getName(),
                          owner,
                          readability,
                          writeability,
                          partitionCount,
                          storageQuotaInByte,
                          readQuotaInCU,
                          currentVersion,
                          hybridRewind,
                          hybridOffsetLag);
      }
    };
  }



  public static Route setOwner(Admin admin) {
    return new VeniceRouteHandler<OwnerResponse>(OwnerResponse.class) {

      @Override
      public void internalHandle(Request request, OwnerResponse veniceResponse) {
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

  public static Route setPartitionCount(Admin admin) {
    return new VeniceRouteHandler<PartitionResponse>(PartitionResponse.class) {

      @Override
      public void internalHandle(Request request, PartitionResponse veniceResponse) {
        AdminSparkServer.validateParams(request, SET_PARTITION_COUNT.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        int partitionNum = Utils.parseIntFromString(request.queryParams(PARTITION_COUNT), "partition-count");
        admin.setStorePartitionCount(clusterName, storeName, partitionNum);

        veniceResponse.setCluster(clusterName);
        veniceResponse.setName(storeName);
        veniceResponse.setPartitionCount(partitionNum);
      }
    };
  }

  public static Route setCurrentVersion(Admin admin) {
    return new VeniceRouteHandler<VersionResponse>(VersionResponse.class) {

      @Override
      public void internalHandle(Request request, VersionResponse veniceResponse) {
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
  public static Route enableStore(Admin admin) {
    return new VeniceRouteHandler<ControllerResponse>(ControllerResponse.class) {

      @Override
      public void internalHandle(Request request, ControllerResponse veniceResponse) {
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

  public static Route deleteAllVersions(Admin admin) {
    return new VeniceRouteHandler<MultiVersionResponse>(MultiVersionResponse.class) {
      @Override
      public void internalHandle(Request request, MultiVersionResponse veniceResponse) {
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

  public static Route getStorageEngineOverheadRatio(Admin admin) {
    return new VeniceRouteHandler<StorageEngineOverheadRatioResponse>(StorageEngineOverheadRatioResponse.class) {

      @Override
      public void internalHandle(Request request, StorageEngineOverheadRatioResponse veniceResponse) {
        AdminSparkServer.validateParams(request, STORAGE_ENGINE_OVERHEAD_RATIO.getParams(), admin);

        veniceResponse.setCluster(request.queryParams(CLUSTER));
        veniceResponse.setName(request.queryParams(NAME));
        veniceResponse.setStorageEngineOverheadRatio(admin.getStorageEngineOverheadRatio(request.queryParams(CLUSTER)));
      }
    };
  }
}
