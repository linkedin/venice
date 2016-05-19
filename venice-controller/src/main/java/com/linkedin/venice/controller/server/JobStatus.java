package com.linkedin.venice.controller.server;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.Utils;
import spark.Route;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.CLUSTER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.JOB_PARMAS;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.NAME;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.VERSION;


/**
 * Created by mwise on 5/18/16.
 */
public class JobStatus {
  public static Route getRoute(Admin admin){
    return (request, response) -> {
      JobStatusQueryResponse responseObject = new JobStatusQueryResponse();
      try {
        AdminSparkServer.validateParams(request, JOB_PARMAS, admin);
        responseObject.setCluster(request.queryParams(CLUSTER));
        responseObject.setName(request.queryParams(NAME));
        responseObject.setVersion(Utils.parseIntFromString(request.queryParams(VERSION), VERSION));
        Version version = new Version(responseObject.getName(), responseObject.getVersion());
        //TODO Support getting streaming job's status in the future.
        String jobStatus = admin.getOffLineJobStatus(responseObject.getCluster(), version.kafkaTopicName()).toString();
        responseObject.setStatus(jobStatus);
      } catch (VeniceException e) {
        responseObject.setError(e.getMessage());
        AdminSparkServer.handleError(e, request, response);
      }
      response.type(HttpConstants.JSON);
      return AdminSparkServer.mapper.writeValueAsString(responseObject);
    };
  }
}
