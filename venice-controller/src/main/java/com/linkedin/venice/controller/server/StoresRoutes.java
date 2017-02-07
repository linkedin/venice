package com.linkedin.venice.controller.server;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.MultiStoreResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreInfo;
import java.util.List;
import spark.Route;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.*;
import static com.linkedin.venice.controllerapi.ControllerRoute.ENABLE_STORE;
import static com.linkedin.venice.controllerapi.ControllerRoute.LIST_STORES;
import static com.linkedin.venice.controllerapi.ControllerRoute.STORE;


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


  public static Route enableStore(Admin admin){
    return (request, response) -> {
      ControllerResponse responseObject = new ControllerResponse();
      try {
        AdminSparkServer.validateParams(request, ENABLE_STORE.getParams(), admin);
        responseObject.setCluster(request.queryParams(CLUSTER));
        responseObject.setName(request.queryParams(NAME));
        String operation = request.queryParams(OPERATION);
        if(operation.equals(READ_OPERATION) || operation.equals(WRITE_OPERATION) || operation.equals(READ_WRITE_OPERATION)){
          if (Boolean.parseBoolean(request.queryParams(STATUS))) { // "true" means enable store
            if(operation.contains(READ_OPERATION)) {
              admin.enableStoreRead(responseObject.getCluster(), responseObject.getName());
            }
            if(operation.contains(WRITE_OPERATION)){
              admin.enableStoreWrite(responseObject.getCluster(), responseObject.getName());
            }
          } else {
            if (operation.contains(READ_OPERATION)) {
              admin.disableStoreRead(responseObject.getCluster(), responseObject.getName());
            }
            if (operation.contains(WRITE_OPERATION)) {
              admin.disableStoreWrite(responseObject.getCluster(), responseObject.getName());
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
}
