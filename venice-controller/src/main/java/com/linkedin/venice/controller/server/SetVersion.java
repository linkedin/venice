package com.linkedin.venice.controller.server;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.Utils;
import java.util.HashMap;
import java.util.Map;
import spark.Route;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.*;


/**
 * Created by mwise on 5/18/16.
 */
public class SetVersion {
  public static Route getRoute(Admin admin) {
    return (request, response) -> {
      Map<String, String> responseMap = new HashMap<>();
      try {
        AdminSparkServer.validateParams(request, SETVERSION_PARAMS, admin); //throws venice exception
        String clusterName = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        int version = Utils.parseIntFromString(request.queryParams(VERSION), VERSION);
        admin.setCurrentVersion(clusterName, storeName, version);
        responseMap.put(STATUS, "success");
      } catch (VeniceException e) {
        responseMap.put(ERROR, e.getMessage());
        AdminSparkServer.handleError(e, request, response);
      }
      response.type(HttpConstants.JSON);
      return AdminSparkServer.mapper.writeValueAsString(responseMap);
    };
  }
}
