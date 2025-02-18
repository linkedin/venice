package com.linkedin.venice.controller.grpc.server;

import static com.linkedin.venice.controller.grpc.server.ControllerGrpcServerUtils.isAllowListUser;
import static com.linkedin.venice.controller.server.VeniceRouteHandler.ACL_CHECK_FAILURE_WARN_MESSAGE_PREFIX;
import static com.linkedin.venice.protocols.controller.ClusterAdminOpsGrpcServiceGrpc.ClusterAdminOpsGrpcServiceImplBase;

import com.linkedin.venice.controller.server.ClusterAdminOpsRequestHandler;
import com.linkedin.venice.controller.server.VeniceControllerAccessManager;
import com.linkedin.venice.exceptions.VeniceUnauthorizedAccessException;
import com.linkedin.venice.protocols.controller.AdminCommandExecutionStatusGrpcRequest;
import com.linkedin.venice.protocols.controller.AdminCommandExecutionStatusGrpcResponse;
import com.linkedin.venice.protocols.controller.AdminTopicGrpcMetadata;
import com.linkedin.venice.protocols.controller.AdminTopicMetadataGrpcRequest;
import com.linkedin.venice.protocols.controller.AdminTopicMetadataGrpcResponse;
import com.linkedin.venice.protocols.controller.ClusterAdminOpsGrpcServiceGrpc;
import com.linkedin.venice.protocols.controller.LastSuccessfulAdminCommandExecutionGrpcRequest;
import com.linkedin.venice.protocols.controller.LastSuccessfulAdminCommandExecutionGrpcResponse;
import com.linkedin.venice.protocols.controller.UpdateAdminOperationProtocolVersionGrpcRequest;
import com.linkedin.venice.protocols.controller.UpdateAdminTopicMetadataGrpcRequest;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ClusterAdminOpsGrpcServiceImpl extends ClusterAdminOpsGrpcServiceImplBase {
  private static final Logger LOGGER = LogManager.getLogger(ClusterAdminOpsGrpcServiceImpl.class);
  private final ClusterAdminOpsRequestHandler requestHandler;
  private final VeniceControllerAccessManager accessManager;

  public ClusterAdminOpsGrpcServiceImpl(
      ClusterAdminOpsRequestHandler requestHandler,
      VeniceControllerAccessManager accessManager) {
    this.requestHandler = requestHandler;
    this.accessManager = accessManager;
  }

  @Override
  public void getAdminCommandExecutionStatus(
      AdminCommandExecutionStatusGrpcRequest request,
      StreamObserver<AdminCommandExecutionStatusGrpcResponse> responseObserver) {
    LOGGER.debug("Received getAdminCommandExecutionStatus request: {}", request);
    ControllerGrpcServerUtils.handleRequest(
        ClusterAdminOpsGrpcServiceGrpc.getGetAdminCommandExecutionStatusMethod(),
        () -> requestHandler.getAdminCommandExecutionStatus(request),
        responseObserver,
        request.getClusterName(),
        null);
  }

  @Override
  public void getLastSuccessfulAdminCommandExecutionId(
      LastSuccessfulAdminCommandExecutionGrpcRequest request,
      StreamObserver<LastSuccessfulAdminCommandExecutionGrpcResponse> responseObserver) {
    LOGGER.debug("Received getLastSuccessfulAdminCommandExecutionId request: {}", request);
    ControllerGrpcServerUtils.handleRequest(
        ClusterAdminOpsGrpcServiceGrpc.getGetLastSuccessfulAdminCommandExecutionIdMethod(),
        () -> requestHandler.getLastSucceedExecutionId(request),
        responseObserver,
        request.getClusterName(),
        null);
  }

  @Override
  public void getAdminTopicMetadata(
      AdminTopicMetadataGrpcRequest request,
      StreamObserver<AdminTopicMetadataGrpcResponse> responseObserver) {
    LOGGER.debug("Received getAdminTopicMetadata request: {}", request);
    ControllerGrpcServerUtils.handleRequest(
        ClusterAdminOpsGrpcServiceGrpc.getGetAdminTopicMetadataMethod(),
        () -> requestHandler.getAdminTopicMetadata(request),
        responseObserver,
        request.getClusterName(),
        request.hasStoreName() ? request.getStoreName() : null);
  }

  @Override
  public void updateAdminTopicMetadata(
      UpdateAdminTopicMetadataGrpcRequest request,
      StreamObserver<AdminTopicMetadataGrpcResponse> responseObserver) {
    LOGGER.debug("Received updateAdminTopicMetadata request: {}", request);
    AdminTopicGrpcMetadata metadata = request.getMetadata();
    ControllerGrpcServerUtils.handleRequest(ClusterAdminOpsGrpcServiceGrpc.getUpdateAdminTopicMetadataMethod(), () -> {
      if (!isAllowListUser(accessManager, request.getMetadata().getStoreName(), Context.current())) {
        throw new VeniceUnauthorizedAccessException(
            ACL_CHECK_FAILURE_WARN_MESSAGE_PREFIX
                + ClusterAdminOpsGrpcServiceGrpc.getUpdateAdminTopicMetadataMethod().getFullMethodName());
      }
      return requestHandler.updateAdminTopicMetadata(request);
    }, responseObserver, metadata.getClusterName(), metadata.hasStoreName() ? metadata.getStoreName() : null);
  }

  @Override
  public void updateAdminOperationProtocolVersion(
      UpdateAdminOperationProtocolVersionGrpcRequest request,
      StreamObserver<AdminTopicMetadataGrpcResponse> responseObserver) {
    LOGGER.debug("Received updateAdminOperationProtocolVersion request: {}", request);
    ControllerGrpcServerUtils.handleRequest(
        ClusterAdminOpsGrpcServiceGrpc.getUpdateAdminOperationProtocolVersionMethod(),
        () -> requestHandler.updateAdminOperationProtocolVersion(request),
        responseObserver,
        request.getClusterName(),
        null);
  }
}
