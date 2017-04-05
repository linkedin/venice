package com.linkedin.venice.controller.server;

import com.linkedin.venice.HttpConstants;
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
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.StoreStatus;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.Utils;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import spark.Route;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.*;
import static com.linkedin.venice.controllerapi.ControllerRoute.*;


public class StoresRoutes {
  public static Route getAllStores(Admin admin) {
    return (request, response) -> {
      MultiStoreResponse responseObject = new MultiStoreResponse();
      try {
        AdminSparkServer.validateParams(request, LIST_STORES.getParams(), admin);
        responseObject.setCluster(request.queryParams(CLUSTER));
        responseObject.setName(request.queryParams(NAME));
        List<Store> storeList = admin.getAllStores(responseObject.getCluster());
        String[] storeNameList = new String[storeList.size()];
        for (int i=0; i<storeList.size(); i++){
          storeNameList[i] = storeList.get(i).getName();
        }
        responseObject.setStores(storeNameList);
      } catch (Throwable e) {
        responseObject.setError(e.getMessage());
        AdminSparkServer.handleError(e, request, response);
      }
      response.type(HttpConstants.JSON);
      return AdminSparkServer.mapper.writeValueAsString(responseObject);
    };
  }

  public static Route getAllStoresStatuses(Admin admin){
    return (request, response) -> {
      MultiStoreStatusResponse responseObject = new MultiStoreStatusResponse();
      try {
        AdminSparkServer.validateParams(request, CLUSTER_HELATH_STORES.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);
        responseObject.setCluster(clusterName);
        Map<String, String> storeStatusMap = admin.getAllStoreStatuses(clusterName);
        responseObject.setStoreStatusMap(storeStatusMap);
      }catch (Throwable e) {
        responseObject.setError(e.getMessage());
        AdminSparkServer.handleError(e, request, response);
      }
      response.type(HttpConstants.JSON);
      return AdminSparkServer.mapper.writeValueAsString(responseObject);
    };
  }

  public static Route getStore(Admin admin){
    return (request, response) -> {
      StoreResponse storeResponse = new StoreResponse();
      try {
        AdminSparkServer.validateParams(request, STORE.getParams(), admin);
        storeResponse.setCluster(request.queryParams(CLUSTER));
        storeResponse.setName(request.queryParams(NAME));
        Store store = admin.getStore(storeResponse.getCluster(), storeResponse.getName());
        if (null == store){
          storeResponse.setError("Store  " + storeResponse.getName() + " does not exist");
        } else {
          storeResponse.setStore(StoreInfo.fromStore(store));
        }
      } catch (Throwable e){
        storeResponse.setError(e.getMessage());
        AdminSparkServer.handleError(e, request, response);
      }
      response.type(HttpConstants.JSON);
      return AdminSparkServer.mapper.writeValueAsString(storeResponse);
    };
  }

  public static Route setOwner(Admin admin) {
    return (request, response) -> {
      OwnerResponse ownerResponse = new OwnerResponse();
      try {
        AdminSparkServer.validateParams(request, SET_OWNER.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        String owner = request.queryParams(OWNER);
        admin.setStoreOwner(clusterName, storeName, owner);

        ownerResponse.setCluster(clusterName);
        ownerResponse.setName(storeName);
        ownerResponse.setOwner(owner);
      } catch (Throwable e) {
        ownerResponse.setError(e.getMessage());
        AdminSparkServer.handleError(e, request, response);
      }

      response.type(HttpConstants.JSON);
      return AdminSparkServer.mapper.writeValueAsString(ownerResponse);
    };
  }

  public static Route setPartitionCount(Admin admin) {
    return (request, response) -> {
      PartitionResponse partitionResponse = new PartitionResponse();
      try {
        AdminSparkServer.validateParams(request, SET_PARTITION_COUNT.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        int partitionNum = Utils.parseIntFromString(request.queryParams(PARTITION_COUNT), "partition-count");
        admin.setStorePartitionCount(clusterName, storeName, partitionNum);

        partitionResponse.setCluster(clusterName);
        partitionResponse.setName(storeName);
        partitionResponse.setPartitionCount(partitionNum);
      } catch (Throwable e) {
        partitionResponse.setError(e.getMessage());
        AdminSparkServer.handleError(e, request, response);
      }

      response.type(HttpConstants.JSON);
      return AdminSparkServer.mapper.writeValueAsString(partitionResponse);
    };
  }

  public static Route setCurrentVersion(Admin admin) {
    return (request, response) -> {
      VersionResponse versionResponse = new VersionResponse();
      try {
        AdminSparkServer.validateParams(request, SET_VERSION.getParams(), admin); //throws venice exception
        String clusterName = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        int version = Utils.parseIntFromString(request.queryParams(VERSION), VERSION);
        admin.setStoreCurrentVersion(clusterName, storeName, version);

        versionResponse.setCluster(clusterName);
        versionResponse.setName(storeName);
        versionResponse.setVersion(version);
      } catch (Throwable e) {
        versionResponse.setError(e.getMessage());
        AdminSparkServer.handleError(e, request, response);
      }

      response.type(HttpConstants.JSON);
      return AdminSparkServer.mapper.writeValueAsString(versionResponse);
    };
  }


  public static Route enableStore(Admin admin){
    return (request, response) -> {
      ControllerResponse responseObject = new ControllerResponse();
      try {
        AdminSparkServer.validateParams(request, ENABLE_STORE.getParams(), admin);
        String cluster = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        String operation = request.queryParams(OPERATION);

        responseObject.setCluster(cluster);
        responseObject.setName(storeName);

        if(operation.equals(READ_OPERATION) || operation.equals(WRITE_OPERATION) || operation.equals(READ_WRITE_OPERATION)){
          if (Boolean.parseBoolean(request.queryParams(STATUS))) { // "true" means enable store
            if(operation.contains(READ_OPERATION)) {
              admin.enableStoreRead(cluster, storeName);
            }
            if(operation.contains(WRITE_OPERATION)){
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
          throw new VeniceException(OPERATION +" parameter:"+operation+" is invalid.");
        }

      } catch (Throwable e) {
        responseObject.setError(e.getMessage());
        AdminSparkServer.handleError(e, request, response);
      }
      response.type(HttpConstants.JSON);
      return AdminSparkServer.mapper.writeValueAsString(responseObject);
    };
  }

  public static Route deleteAllVersions(Admin admin) {
    return (request, response) -> {
      MultiVersionResponse responseObject = new MultiVersionResponse();
      try {
        AdminSparkServer.validateParams(request, DELETE_ALL_VERSIONS.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        responseObject.setCluster(clusterName);
        responseObject.setName(storeName);
        List<Version> deletedVersions = Collections.emptyList();
        Optional<AdminCommandExecutionTracker> adminCommandExecutionTracker = admin.getAdminCommandExecutionTracker();
        if (adminCommandExecutionTracker.isPresent()) {
          // Lock the tracker to get the execution id for the last admin command.
          // If will not make our perfomrance worse, because we lock the whole cluster while handling the admin operation in parent admin.
          synchronized (adminCommandExecutionTracker) {
            deletedVersions = admin.deleteAllVersionsInStore(clusterName, storeName);
            responseObject.setExecutionId(adminCommandExecutionTracker.get().getLastExecutionId());
          }
        } else {
          deletedVersions = admin.deleteAllVersionsInStore(clusterName, storeName);
        }

        int[] deletedVersionNumbers = new int[deletedVersions.size()];
        for (int i = 0; i < deletedVersions.size(); i++) {
          deletedVersionNumbers[i] = deletedVersions.get(i).getNumber();
        }
        responseObject.setVersions(deletedVersionNumbers);
      } catch (Throwable e) {
        responseObject.setError(e.getMessage());
        AdminSparkServer.handleError(e, request, response);
      }
      response.type(HttpConstants.JSON);
      return AdminSparkServer.mapper.writeValueAsString(responseObject);
    };
  }
}
