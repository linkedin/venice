package com.linkedin.venice.controller.server;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.CLUSTER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.EXECUTION_ID;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.NAME;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.OFFSET;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.UPSTREAM_OFFSET;
import static com.linkedin.venice.controllerapi.ControllerRoute.GET_ADMIN_TOPIC_METADATA;
import static com.linkedin.venice.controllerapi.ControllerRoute.UPDATE_ADMIN_TOPIC_METADATA;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.AdminTopicMetadataResponse;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.request.AdminTopicMetadataRequest;
import com.linkedin.venice.controllerapi.request.UpdateAdminTopicMetadataRequest;
import com.linkedin.venice.exceptions.ErrorType;
import com.linkedin.venice.exceptions.VeniceException;
import java.util.Optional;
import org.apache.http.HttpStatus;
import spark.Route;


public class AdminTopicMetadataRoutes extends AbstractRoute {
  public AdminTopicMetadataRoutes(
      boolean sslEnabled,
      Optional<DynamicAccessController> accessController,
      VeniceControllerRequestHandler requestHandler) {
    super(sslEnabled, accessController, requestHandler);
  }

  /**
   * @see Admin#getAdminTopicMetadata(String, Optional)
   */
  public Route getAdminTopicMetadata(Admin admin) {
    return (request, response) -> {
      AdminTopicMetadataResponse responseObject = new AdminTopicMetadataResponse();
      response.type(HttpConstants.JSON);
      try {
        // No ACL check on getting admin topic metadata
        AdminSparkServer.validateParams(request, GET_ADMIN_TOPIC_METADATA.getParams(), admin);
        AdminTopicMetadataRequest adminTopicMetadataRequest =
            new AdminTopicMetadataRequest(request.queryParams(CLUSTER), request.queryParams(NAME));
        requestHandler.getAdminTopicMetadata(adminTopicMetadataRequest, responseObject);
      } catch (Throwable e) {
        responseObject.setError(e);
        AdminSparkServer.handleError(new VeniceException(e), request, response);
      }
      return AdminSparkServer.OBJECT_MAPPER.writeValueAsString(responseObject);
    };
  }

  /**
   * @see Admin#updateAdminTopicMetadata(String, long, Optional, Optional, Optional)
   */
  public Route updateAdminTopicMetadata(Admin admin) {
    return (request, response) -> {
      ControllerResponse responseObject = new ControllerResponse();
      response.type(HttpConstants.JSON);
      try {
        if (!isAllowListUser(request)) {
          response.status(HttpStatus.SC_FORBIDDEN);
          responseObject.setError("Only admin users are allowed to run " + request.url());
          responseObject.setErrorType(ErrorType.BAD_REQUEST);
          return AdminSparkServer.OBJECT_MAPPER.writeValueAsString(responseObject);
        }
        AdminSparkServer.validateParams(request, UPDATE_ADMIN_TOPIC_METADATA.getParams(), admin);
        UpdateAdminTopicMetadataRequest updateAdminTopicMetadataRequest = new UpdateAdminTopicMetadataRequest(
            request.queryParams(CLUSTER),
            request.queryParams(NAME),
            request.queryParams(EXECUTION_ID),
            request.queryParams(OFFSET),
            request.queryParams(UPSTREAM_OFFSET));
        requestHandler.updateAdminTopicMetadata(updateAdminTopicMetadataRequest, responseObject);
      } catch (Throwable e) {
        responseObject.setError(e);
        AdminSparkServer.handleError(new VeniceException(e), request, response);
      }
      return AdminSparkServer.OBJECT_MAPPER.writeValueAsString(responseObject);
    };
  }
}
