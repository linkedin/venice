package com.linkedin.venice.controller.server;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.CLUSTER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.EXECUTION_ID;
import static com.linkedin.venice.controllerapi.ControllerRoute.EXECUTION;
import static com.linkedin.venice.controllerapi.ControllerRoute.LAST_SUCCEED_EXECUTION_ID;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.LastSucceedExecutionIdResponse;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.grpc.GrpcRequestResponseConverter;
import com.linkedin.venice.controllerapi.AdminCommandExecution;
import com.linkedin.venice.controllerapi.AdminCommandExecutionStatus;
import com.linkedin.venice.controllerapi.routes.AdminCommandExecutionResponse;
import com.linkedin.venice.protocols.controller.AdminCommandExecutionStatusGrpcRequest;
import com.linkedin.venice.protocols.controller.AdminCommandExecutionStatusGrpcResponse;
import com.linkedin.venice.protocols.controller.LastSuccessfulAdminCommandExecutionGrpcRequest;
import com.linkedin.venice.protocols.controller.LastSuccessfulAdminCommandExecutionGrpcResponse;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import spark.Route;


public class AdminCommandExecutionRoutes extends AbstractRoute {
  public AdminCommandExecutionRoutes(boolean sslEnabled, Optional<DynamicAccessController> accessController) {
    super(sslEnabled, accessController);
  }

  /**
   * No ACL check; any user is allowed to check admin command execution status.
   * @see Admin#getAdminCommandExecutionTracker(String)
   */
  public Route getExecution(Admin admin, ClusterAdminOpsRequestHandler requestHandler) {
    return (request, response) -> {
      AdminCommandExecutionResponse responseObject = new AdminCommandExecutionResponse();
      response.type(HttpConstants.JSON);
      try {
        // This request should only hit the parent controller. If a PROD controller get this kind of request, a empty
        // response would be return.
        AdminSparkServer.validateParams(request, EXECUTION.getParams(), admin);
        String cluster = request.queryParams(CLUSTER);
        long executionId = Long.parseLong(request.queryParams(EXECUTION_ID));
        AdminCommandExecutionStatusGrpcRequest.Builder requestBuilder =
            AdminCommandExecutionStatusGrpcRequest.newBuilder()
                .setClusterName(cluster)
                .setAdminCommandExecutionId(executionId);
        AdminCommandExecutionStatusGrpcResponse grpcResponse =
            requestHandler.getAdminCommandExecutionStatus(requestBuilder.build());
        responseObject.setCluster(cluster);
        AdminCommandExecution execution = new AdminCommandExecution();
        execution.setExecutionId(executionId);
        execution.setOperation(grpcResponse.getOperation());
        execution.setClusterName(grpcResponse.getClusterName());
        execution.setStartTime(grpcResponse.getStartTime());
        Map<String, String> fabricToExecutionStatusMap = grpcResponse.getFabricToExecutionStatusMapMap();
        ConcurrentHashMap<String, AdminCommandExecutionStatus> executionStatusMap =
            GrpcRequestResponseConverter.toExecutionStatusMap(fabricToExecutionStatusMap);
        execution.setFabricToExecutionStatusMap(executionStatusMap);
        responseObject.setExecution(execution);
      } catch (Throwable e) {
        responseObject.setError(e.getMessage());
        AdminSparkServer.handleError(e, request, response);
      }
      return AdminSparkServer.OBJECT_MAPPER.writeValueAsString(responseObject);
    };
  }

  /**
   * No ACL check; any user is allowed to check last succeeded execution Id.
   * @see Admin#getLastSucceedExecutionId(String)
   */
  public Route getLastSucceedExecutionId(Admin admin, ClusterAdminOpsRequestHandler requestHandler) {
    return (request, response) -> {
      LastSucceedExecutionIdResponse responseObject = new LastSucceedExecutionIdResponse();
      response.type(HttpConstants.JSON);
      try {
        AdminSparkServer.validateParams(request, LAST_SUCCEED_EXECUTION_ID.getParams(), admin);
        String cluster = request.queryParams(CLUSTER);
        responseObject.setCluster(cluster);
        LastSuccessfulAdminCommandExecutionGrpcResponse grpcResponse = requestHandler.getLastSucceedExecutionId(
            LastSuccessfulAdminCommandExecutionGrpcRequest.newBuilder().setClusterName(cluster).build());
        responseObject.setLastSucceedExecutionId(grpcResponse.getLastSuccessfulAdminCommandExecutionId());
      } catch (Throwable e) {
        responseObject.setError(e);
        AdminSparkServer.handleError(e, request, response);
      }
      return AdminSparkServer.OBJECT_MAPPER.writeValueAsString(responseObject);
    };
  }
}
