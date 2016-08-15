package com.linkedin.venice.controller.server;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.MultiStoreResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Store;
import java.util.List;
import spark.Route;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.CLUSTER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.NAME;
import static com.linkedin.venice.controllerapi.ControllerRoute.LIST_STORES;


/**
 * Created by mwise on 5/18/16.
 */
public class AllStores {
  public static Route getRoute(Admin admin) {
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
      } catch (VeniceException e) {
        responseObject.setError(e.getMessage());
        AdminSparkServer.handleError(e, request, response);
      }
      response.type(HttpConstants.JSON);
      return AdminSparkServer.mapper.writeValueAsString(responseObject);
    };
  }
}
