package com.linkedin.venice.controller.server;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.VersionResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.Utils;
import spark.Route;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.CLUSTER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.NAME;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.VERSION;
import static com.linkedin.venice.controllerapi.ControllerRoute.SETVERSION;


/**
 * Created by mwise on 5/18/16.
 */
public class SetVersion {
  public static Route getRoute(Admin admin) {
    return (request, response) -> {
      VersionResponse responseObj = new VersionResponse();
      try {
        AdminSparkServer.validateParams(request, SETVERSION.getParams(), admin); //throws venice exception
        String clusterName = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        int version = Utils.parseIntFromString(request.queryParams(VERSION), VERSION);
        responseObj.setCluster(clusterName);
        responseObj.setVersion(version);
        responseObj.setName(storeName);
        admin.setCurrentVersion(clusterName, storeName, version);
      } catch (VeniceException e) {
        responseObj.setError(e.getMessage());
        AdminSparkServer.handleError(e, request, response);
      }
      response.type(HttpConstants.JSON);
      return AdminSparkServer.mapper.writeValueAsString(responseObj);
    };
  }
}
