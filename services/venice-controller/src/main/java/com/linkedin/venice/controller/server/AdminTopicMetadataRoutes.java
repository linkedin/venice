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
import com.linkedin.venice.authentication.AuthenticationService;
import com.linkedin.venice.authorization.AuthorizerService;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.AdminTopicMetadataAccessor;
import com.linkedin.venice.controllerapi.AdminTopicMetadataResponse;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.exceptions.ErrorType;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.Pair;
import java.util.Map;
import java.util.Optional;
import org.apache.http.HttpStatus;
import spark.Route;


public class AdminTopicMetadataRoutes extends AbstractRoute {
  public AdminTopicMetadataRoutes(
      boolean sslEnabled,
      Optional<DynamicAccessController> accessController,
      Optional<AuthenticationService> authenticationService,
      Optional<AuthorizerService> authorizerService) {
    super(sslEnabled, accessController, authenticationService, authorizerService);
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
        String clusterName = request.queryParams(CLUSTER);
        Optional<String> storeName = Optional.ofNullable(request.queryParams(NAME));

        responseObject.setCluster(clusterName);
        storeName.ifPresent(responseObject::setName);

        Map<String, Long> metadata = admin.getAdminTopicMetadata(clusterName, storeName);

        responseObject.setExecutionId(AdminTopicMetadataAccessor.getExecutionId(metadata));
        if (!storeName.isPresent()) {
          Pair<Long, Long> offsets = AdminTopicMetadataAccessor.getOffsets(metadata);
          responseObject.setOffset(offsets.getFirst());
          responseObject.setUpstreamOffset(offsets.getSecond());
        }
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
        String clusterName = request.queryParams(CLUSTER);
        long executionId = Long.parseLong(request.queryParams(EXECUTION_ID));
        Optional<String> storeName = Optional.ofNullable(request.queryParams(NAME));
        Optional<Long> offset = Optional.ofNullable(request.queryParams(OFFSET)).map(Long::parseLong);
        Optional<Long> upstreamOffset = Optional.ofNullable(request.queryParams(UPSTREAM_OFFSET)).map(Long::parseLong);

        if (storeName.isPresent()) {
          if (offset.isPresent() || upstreamOffset.isPresent()) {
            throw new VeniceException("There is no store-level offsets to be updated");
          }
        } else {
          if (!offset.isPresent() || !upstreamOffset.isPresent()) {
            throw new VeniceException("Offsets must be provided to update cluster-level admin topic metadata");
          }
        }

        responseObject.setCluster(clusterName);
        storeName.ifPresent(responseObject::setName);

        admin.updateAdminTopicMetadata(clusterName, executionId, storeName, offset, upstreamOffset);
      } catch (Throwable e) {
        responseObject.setError(e);
        AdminSparkServer.handleError(new VeniceException(e), request, response);
      }
      return AdminSparkServer.OBJECT_MAPPER.writeValueAsString(responseObject);
    };
  }
}
