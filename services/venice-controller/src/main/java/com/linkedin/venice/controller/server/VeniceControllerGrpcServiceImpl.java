package com.linkedin.venice.controller.server;

import static com.linkedin.venice.controller.server.VeniceRouteHandler.ACL_CHECK_FAILURE_WARN_MESSAGE_PREFIX;
import static com.linkedin.venice.controller.server.grpc.ControllerGrpcSslSessionInterceptor.GRPC_CONTROLLER_CLIENT_DETAILS;

import com.linkedin.venice.controller.server.grpc.GrpcControllerClientDetails;
import com.linkedin.venice.controllerapi.transport.GrpcRequestResponseConverter;
import com.linkedin.venice.protocols.controller.ControllerGrpcErrorType;
import com.linkedin.venice.protocols.controller.CreateStoreGrpcRequest;
import com.linkedin.venice.protocols.controller.CreateStoreGrpcResponse;
import com.linkedin.venice.protocols.controller.DiscoverClusterGrpcRequest;
import com.linkedin.venice.protocols.controller.DiscoverClusterGrpcResponse;
import com.linkedin.venice.protocols.controller.LeaderControllerGrpcRequest;
import com.linkedin.venice.protocols.controller.LeaderControllerGrpcResponse;
import com.linkedin.venice.protocols.controller.VeniceControllerGrpcServiceGrpc;
import com.linkedin.venice.protocols.controller.VeniceControllerGrpcServiceGrpc.VeniceControllerGrpcServiceImplBase;
import io.grpc.Context;
import io.grpc.Status.Code;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class is a gRPC service implementation for the VeniceController public API.
 */
public class VeniceControllerGrpcServiceImpl extends VeniceControllerGrpcServiceImplBase {
  private static final Logger LOGGER = LogManager.getLogger(VeniceControllerGrpcServiceImpl.class);

  private final VeniceControllerRequestHandler requestHandler;
  private final VeniceControllerAccessManager accessManager;

  public VeniceControllerGrpcServiceImpl(VeniceControllerRequestHandler requestHandler) {
    this.requestHandler = requestHandler;
    this.accessManager = requestHandler.getControllerAccessManager();
  }

  protected GrpcControllerClientDetails getClientDetails(Context context) {
    GrpcControllerClientDetails clientDetails = GRPC_CONTROLLER_CLIENT_DETAILS.get(context);
    if (clientDetails == null) {
      clientDetails = GrpcControllerClientDetails.UNDEFINED_CLIENT_DETAILS;
    }
    return clientDetails;
  }

  public boolean isAllowListUser(String resourceName, Context context) {
    GrpcControllerClientDetails clientDetails = getClientDetails(context);
    return accessManager.isAllowListUser(resourceName, clientDetails.getClientCertificate());
  }

  @Override
  public void getLeaderController(
      LeaderControllerGrpcRequest request,
      StreamObserver<LeaderControllerGrpcResponse> responseObserver) {
    String clusterName = request.getClusterName();
    LOGGER.info("Received gRPC request to get leader controller for cluster: {}", clusterName);
    try {
      responseObserver.onNext(requestHandler.getLeaderControllerDetails(request));
      responseObserver.onCompleted();
    } catch (IllegalArgumentException e) {
      LOGGER.error("Invalid argument while getting leader controller for cluster: {}", clusterName, e);
      GrpcRequestResponseConverter.sendErrorResponse(
          Code.INVALID_ARGUMENT,
          ControllerGrpcErrorType.BAD_REQUEST,
          e,
          clusterName,
          null,
          responseObserver);
    } catch (Exception e) {
      LOGGER.error("Error while getting leader controller for cluster: {}", clusterName, e);
      GrpcRequestResponseConverter.sendErrorResponse(
          Code.INTERNAL,
          ControllerGrpcErrorType.GENERAL_ERROR,
          e,
          clusterName,
          null,
          responseObserver);
    }
  }

  @Override
  public void discoverClusterForStore(
      DiscoverClusterGrpcRequest grpcRequest,
      StreamObserver<DiscoverClusterGrpcResponse> responseObserver) {
    String storeName = grpcRequest.getStoreName();
    LOGGER.info("Received gRPC request to discover cluster for store: {}", storeName);
    try {
      responseObserver.onNext(requestHandler.discoverCluster(grpcRequest));
      responseObserver.onCompleted();
    } catch (IllegalArgumentException e) {
      LOGGER.error("Invalid argument while discovering cluster for store: {}", storeName, e);
      GrpcRequestResponseConverter.sendErrorResponse(
          Code.INVALID_ARGUMENT,
          ControllerGrpcErrorType.BAD_REQUEST,
          e,
          null,
          storeName,
          responseObserver);
    } catch (Exception e) {
      LOGGER.error("Error while discovering cluster for store: {}", storeName, e);
      GrpcRequestResponseConverter.sendErrorResponse(
          Code.INTERNAL,
          ControllerGrpcErrorType.GENERAL_ERROR,
          e,
          null,
          storeName,
          responseObserver);
    }
  }

  @Override
  public void createStore(
      CreateStoreGrpcRequest grpcRequest,
      StreamObserver<CreateStoreGrpcResponse> responseObserver) {
    String clusterName = grpcRequest.getClusterStoreInfo().getClusterName();
    String storeName = grpcRequest.getClusterStoreInfo().getStoreName();
    LOGGER.info("Received gRPC request to create store: {} in cluster: {}", storeName, clusterName);
    try {
      if (!isAllowListUser(storeName, Context.current())) {
        GrpcRequestResponseConverter.sendErrorResponse(
            Code.PERMISSION_DENIED,
            ControllerGrpcErrorType.UNAUTHORIZED,
            ACL_CHECK_FAILURE_WARN_MESSAGE_PREFIX
                + VeniceControllerGrpcServiceGrpc.getCreateStoreMethod().getFullMethodName(),
            clusterName,
            storeName,
            responseObserver);
        return;
      }
      responseObserver.onNext(requestHandler.createStore(grpcRequest));
      responseObserver.onCompleted();
    } catch (IllegalArgumentException e) {
      LOGGER.error("Invalid argument while creating store: {} in cluster: {}", storeName, clusterName, e);
      GrpcRequestResponseConverter.sendErrorResponse(
          Code.INVALID_ARGUMENT,
          ControllerGrpcErrorType.BAD_REQUEST,
          e,
          clusterName,
          storeName,
          responseObserver);
    } catch (Exception e) {
      LOGGER.error("Error while creating store: {} in cluster: {}", storeName, clusterName, e);
      GrpcRequestResponseConverter.sendErrorResponse(
          Code.INTERNAL,
          ControllerGrpcErrorType.GENERAL_ERROR,
          e,
          clusterName,
          storeName,
          responseObserver);
    }
  }
}
