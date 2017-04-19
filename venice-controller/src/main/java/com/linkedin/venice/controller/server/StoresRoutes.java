package com.linkedin.venice.controller.server;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.AdminCommandExecutionTracker;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.MultiStoreResponse;
import com.linkedin.venice.controllerapi.MultiStoreStatusResponse;
import com.linkedin.venice.controllerapi.MultiVersionResponse;
import com.linkedin.venice.controllerapi.OwnerResponse;
import com.linkedin.venice.controllerapi.PartitionResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
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
      public void internalHandle(Request request, MultiStoreResponse veniceRepsonse) {
        AdminSparkServer.validateParams(request, LIST_STORES.getParams(), admin);
        veniceRepsonse.setCluster(request.queryParams(CLUSTER));
        veniceRepsonse.setName(request.queryParams(NAME));
        List<Store> storeList = admin.getAllStores(veniceRepsonse.getCluster());
        String[] storeNameList = new String[storeList.size()];
        for (int i = 0; i < storeList.size(); i++) {
          storeNameList[i] = storeList.get(i).getName();
        }
        veniceRepsonse.setStores(storeNameList);
      }
    };
  }

  public static Route getAllStoresStatuses(Admin admin) {
    return new VeniceRouteHandler<MultiStoreStatusResponse>(MultiStoreStatusResponse.class) {
      @Override
      public void internalHandle(Request request, MultiStoreStatusResponse veniceRepsonse) {
        AdminSparkServer.validateParams(request, CLUSTER_HELATH_STORES.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);
        veniceRepsonse.setCluster(clusterName);
        Map<String, String> storeStatusMap = admin.getAllStoreStatuses(clusterName);
        veniceRepsonse.setStoreStatusMap(storeStatusMap);
      }
    };
  }

  public static Route getStore(Admin admin) {
    return new VeniceRouteHandler<StoreResponse>(StoreResponse.class) {

      @Override
      public void internalHandle(Request request, StoreResponse veniceRepsonse) {
        AdminSparkServer.validateParams(request, STORE.getParams(), admin);
        veniceRepsonse.setCluster(request.queryParams(CLUSTER));
        veniceRepsonse.setName(request.queryParams(NAME));
        Store store = admin.getStore(veniceRepsonse.getCluster(), veniceRepsonse.getName());
        if (null == store) {
          throw new VeniceNoStoreException(veniceRepsonse.getName());
        }
        veniceRepsonse.setStore(StoreInfo.fromStore(store));
      }
    };
  }

  public static Route updateStore(Admin admin) {
    return new VeniceRouteHandler<ControllerResponse>(ControllerResponse.class) {

      @Override
      public void internalHandle(Request request, ControllerResponse veniceRepsonse) {
        AdminSparkServer.validateParams(request, UPDATE_STORE.getParams(), admin);
        veniceRepsonse.setCluster(request.queryParams(CLUSTER));
        veniceRepsonse.setName(request.queryParams(NAME));

        admin.storeMetadataUpdate(veniceRepsonse.getCluster(), veniceRepsonse.getName(), store->{
          String owner = AdminSparkServer.getOptionalParameterValue(request, OWNER, store.getOwner());
          String principles = AdminSparkServer.getOptionalParameterValue(request, PRINCIPLES, String.join(",", store.getPrinciples()));
          int partitionCount = Utils.parseIntFromString(
              AdminSparkServer.getOptionalParameterValue(request, PARTITION_COUNT,
                  String.valueOf(store.getPartitionCount())), "partitionCount");
          int currentVersion = Utils.parseIntFromString(
              AdminSparkServer.getOptionalParameterValue(request, VERSION, String.valueOf(store.getCurrentVersion())),
              "currentVersion");
          boolean enableWrites = Utils.parseBooleanFromString(
              AdminSparkServer.getOptionalParameterValue(request, ENABLE_WRITES, String.valueOf(store.isEnableReads())),
              "enableReads");
          boolean enableReads = Utils.parseBooleanFromString(
              AdminSparkServer.getOptionalParameterValue(request, ENABLE_READS, String.valueOf(store.isEnableReads())),
              "enableWrites");

          store.setOwner(owner);
          store.setPrinciples(Utils.parseCommaSeparatedStringToList(principles));
          store.setPartitionCount(partitionCount);
          store.setCurrentVersion(currentVersion);
          store.setEnableWrites(enableWrites);
          store.setEnableReads(enableReads);
          return store;
        });
      }
    };
  }

  public static Route setOwner(Admin admin) {
    return new VeniceRouteHandler<OwnerResponse>(OwnerResponse.class) {

      @Override
      public void internalHandle(Request request, OwnerResponse veniceRepsonse) {
        AdminSparkServer.validateParams(request, SET_OWNER.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        String owner = request.queryParams(OWNER);
        admin.setStoreOwner(clusterName, storeName, owner);

        veniceRepsonse.setCluster(clusterName);
        veniceRepsonse.setName(storeName);
        veniceRepsonse.setOwner(owner);
      }
    };
  }

  public static Route setPartitionCount(Admin admin) {
    return new VeniceRouteHandler<PartitionResponse>(PartitionResponse.class) {

      @Override
      public void internalHandle(Request request, PartitionResponse veniceRepsonse) {
        AdminSparkServer.validateParams(request, SET_PARTITION_COUNT.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        int partitionNum = Utils.parseIntFromString(request.queryParams(PARTITION_COUNT), "partition-count");
        admin.setStorePartitionCount(clusterName, storeName, partitionNum);

        veniceRepsonse.setCluster(clusterName);
        veniceRepsonse.setName(storeName);
        veniceRepsonse.setPartitionCount(partitionNum);
      }
    };
  }

  public static Route setCurrentVersion(Admin admin) {
    return new VeniceRouteHandler<VersionResponse>(VersionResponse.class) {

      @Override
      public void internalHandle(Request request, VersionResponse veniceRepsonse) {
        AdminSparkServer.validateParams(request, SET_VERSION.getParams(), admin); //throws venice exception
        String clusterName = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        int version = Utils.parseIntFromString(request.queryParams(VERSION), VERSION);
        admin.setStoreCurrentVersion(clusterName, storeName, version);

        veniceRepsonse.setCluster(clusterName);
        veniceRepsonse.setName(storeName);
        veniceRepsonse.setVersion(version);
      }
    };
  }

  public static Route enableStore(Admin admin) {
    return new VeniceRouteHandler<ControllerResponse>(ControllerResponse.class) {

      @Override
      public void internalHandle(Request request, ControllerResponse veniceRepsonse) {
        AdminSparkServer.validateParams(request, ENABLE_STORE.getParams(), admin);
        String cluster = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        String operation = request.queryParams(OPERATION);

        veniceRepsonse.setCluster(cluster);
        veniceRepsonse.setName(storeName);

        if (operation.equals(READ_OPERATION) || operation.equals(WRITE_OPERATION) || operation.equals(
            READ_WRITE_OPERATION)) {
          if (Boolean.parseBoolean(request.queryParams(STATUS))) { // "true" means enable store
            if (operation.contains(READ_OPERATION)) {
              admin.enableStoreRead(cluster, storeName);
            }
            if (operation.contains(WRITE_OPERATION)) {
              admin.enableStoreWrite(cluster, storeName);
            }
          } else {
            if (operation.contains(READ_OPERATION)) {
              admin.disableStoreRead(cluster, storeName);
            }
            if (operation.contains(WRITE_OPERATION)) {
              admin.disableStoreWrite(cluster, storeName);
            }
          }
        } else {
          throw new VeniceException(OPERATION + " parameter:" + operation + " is invalid.");
        }
      }
    };
  }

  public static Route deleteAllVersions(Admin admin) {
    return new VeniceRouteHandler<MultiVersionResponse>(MultiVersionResponse.class) {
      @Override
      public void internalHandle(Request request, MultiVersionResponse veniceRepsonse) {
        AdminSparkServer.validateParams(request, DELETE_ALL_VERSIONS.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        veniceRepsonse.setCluster(clusterName);
        veniceRepsonse.setName(storeName);
        List<Version> deletedVersions = Collections.emptyList();
        Optional<AdminCommandExecutionTracker> adminCommandExecutionTracker = admin.getAdminCommandExecutionTracker();
        if (adminCommandExecutionTracker.isPresent()) {
          // Lock the tracker to get the execution id for the last admin command.
          // If will not make our perfomrance worse, because we lock the whole cluster while handling the admin operation in parent admin.
          synchronized (adminCommandExecutionTracker) {
            deletedVersions = admin.deleteAllVersionsInStore(clusterName, storeName);
            veniceRepsonse.setExecutionId(adminCommandExecutionTracker.get().getLastExecutionId());
          }
        } else {
          deletedVersions = admin.deleteAllVersionsInStore(clusterName, storeName);
        }

        int[] deletedVersionNumbers = new int[deletedVersions.size()];
        for (int i = 0; i < deletedVersions.size(); i++) {
          deletedVersionNumbers[i] = deletedVersions.get(i).getNumber();
        }
        veniceRepsonse.setVersions(deletedVersionNumbers);
      }
    };
  }
}
