package com.linkedin.venice.controller.server;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.MultiVersionResponse;
import com.linkedin.venice.controllerapi.VersionResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import java.util.ArrayList;
import java.util.List;
import spark.Route;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.CLUSTER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.CURRENT_VERSION_PARAMS;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.NAME;


/**
 * Created by mwise on 5/18/16.
 */
public class ActiveVersions {
  public static Route getRoute(Admin admin) {
    return (request, response) -> {
      MultiVersionResponse responseObject = new MultiVersionResponse();
      try {
        AdminSparkServer.validateParams(request, CURRENT_VERSION_PARAMS, admin);
        responseObject.setCluster(request.queryParams(CLUSTER));
        responseObject.setName(request.queryParams(NAME));
        List<Version> versionsList = admin.versionsForStore(responseObject.getCluster(), responseObject.getName());
        List<Integer> activeVersions = new ArrayList<>();
        for (Version version : versionsList){
          if (version.getStatus().equals(VersionStatus.ACTIVE)){
            activeVersions.add(version.getNumber());
          }
        }
        int[] versions = new int[activeVersions.size()];
        for (int i=0; i<activeVersions.size(); i++){
          versions[i] = activeVersions.get(i);
        }
        responseObject.setVersions(versions);
      } catch (VeniceException e) {
        responseObject.setError(e.getMessage());
        AdminSparkServer.handleError(e, request, response);
      }
      response.type(HttpConstants.JSON);
      return AdminSparkServer.mapper.writeValueAsString(responseObject);
    };
  }
}
