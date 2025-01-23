package com.linkedin.venice.controller.grpc.server;

import static com.linkedin.venice.controller.grpc.server.ControllerGrpcServerUtils.handleRequest;
import static com.linkedin.venice.controller.grpc.server.ControllerGrpcServerUtils.isAllowListUser;
import static com.linkedin.venice.controller.server.VeniceRouteHandler.ACL_CHECK_FAILURE_WARN_MESSAGE_PREFIX;

import com.linkedin.venice.controller.server.StoreRequestHandler;
import com.linkedin.venice.controller.server.VeniceControllerAccessManager;
import com.linkedin.venice.exceptions.VeniceUnauthorizedAccessException;
import com.linkedin.venice.protocols.controller.ClusterStoreGrpcInfo;
import com.linkedin.venice.protocols.controller.CreateStoreGrpcRequest;
import com.linkedin.venice.protocols.controller.CreateStoreGrpcResponse;
import com.linkedin.venice.protocols.controller.DeleteAclForStoreGrpcRequest;
import com.linkedin.venice.protocols.controller.DeleteAclForStoreGrpcResponse;
import com.linkedin.venice.protocols.controller.GetAclForStoreGrpcRequest;
import com.linkedin.venice.protocols.controller.GetAclForStoreGrpcResponse;
import com.linkedin.venice.protocols.controller.ResourceCleanupCheckGrpcResponse;
import com.linkedin.venice.protocols.controller.StoreGrpcServiceGrpc;
import com.linkedin.venice.protocols.controller.StoreGrpcServiceGrpc.StoreGrpcServiceImplBase;
import com.linkedin.venice.protocols.controller.UpdateAclForStoreGrpcRequest;
import com.linkedin.venice.protocols.controller.UpdateAclForStoreGrpcResponse;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class StoreGrpcServiceImpl extends StoreGrpcServiceImplBase {
  private static final Logger LOGGER = LogManager.getLogger(StoreGrpcServiceImpl.class);
  private final StoreRequestHandler storeRequestHandler;
  private final VeniceControllerAccessManager accessManager;

  public StoreGrpcServiceImpl(StoreRequestHandler storeRequestHandler, VeniceControllerAccessManager accessManager) {
    this.storeRequestHandler = storeRequestHandler;
    this.accessManager = accessManager;
  }

  @Override
  public void createStore(
      CreateStoreGrpcRequest grpcRequest,
      StreamObserver<CreateStoreGrpcResponse> responseObserver) {
    LOGGER.debug("Received createStore with args: {}", grpcRequest);
    String clusterName = grpcRequest.getStoreInfo().getClusterName();
    String storeName = grpcRequest.getStoreInfo().getStoreName();
    handleRequest(StoreGrpcServiceGrpc.getCreateStoreMethod(), () -> {
      if (!isAllowListUser(accessManager, grpcRequest.getStoreInfo().getStoreName(), Context.current())) {
        throw new VeniceUnauthorizedAccessException(
            ACL_CHECK_FAILURE_WARN_MESSAGE_PREFIX + StoreGrpcServiceGrpc.getCreateStoreMethod().getFullMethodName()
                + " on resource: " + storeName);
      }
      return storeRequestHandler.createStore(grpcRequest);
    }, responseObserver, clusterName, storeName);
  }

  @Override
  public void updateAclForStore(
      UpdateAclForStoreGrpcRequest request,
      StreamObserver<UpdateAclForStoreGrpcResponse> responseObserver) {
    LOGGER.debug("Received updateAclForStore with args: {}", request);
    ControllerGrpcServerUtils.handleRequest(
        StoreGrpcServiceGrpc.getUpdateAclForStoreMethod(),
        () -> storeRequestHandler.updateAclForStore(request),
        responseObserver,
        request.getStoreInfo());
  }

  @Override
  public void getAclForStore(
      GetAclForStoreGrpcRequest request,
      StreamObserver<GetAclForStoreGrpcResponse> responseObserver) {
    LOGGER.debug("Received getAclForStore with args: {}", request);
    ControllerGrpcServerUtils.handleRequest(
        StoreGrpcServiceGrpc.getGetAclForStoreMethod(),
        () -> storeRequestHandler.getAclForStore(request),
        responseObserver,
        request.getStoreInfo());
  }

  @Override
  public void deleteAclForStore(
      DeleteAclForStoreGrpcRequest request,
      StreamObserver<DeleteAclForStoreGrpcResponse> responseObserver) {
    LOGGER.debug("Received deleteAclForStore with args: {}", request);
    ControllerGrpcServerUtils.handleRequest(
        StoreGrpcServiceGrpc.getDeleteAclForStoreMethod(),
        () -> storeRequestHandler.deleteAclForStore(request),
        responseObserver,
        request.getStoreInfo());
  }

  @Override
  public void checkResourceCleanupForStoreCreation(
      ClusterStoreGrpcInfo request,
      StreamObserver<ResourceCleanupCheckGrpcResponse> responseObserver) {
    LOGGER.debug("Received checkResourceCleanupForStoreCreation with args: {}", request);
    ControllerGrpcServerUtils
        .handleRequest(StoreGrpcServiceGrpc.getCheckResourceCleanupForStoreCreationMethod(), () -> {
          ResourceCleanupCheckGrpcResponse.Builder responseBuilder =
              ResourceCleanupCheckGrpcResponse.newBuilder().setStoreInfo(request);
          try {
            storeRequestHandler.checkResourceCleanupForStoreCreation(request);
            responseBuilder.setHasLingeringResources(false);
          } catch (Exception e) {
            responseBuilder.setHasLingeringResources(true);
            if (e.getMessage() != null) {
              responseBuilder.setDescription(e.getMessage());
            }
          }
          return responseBuilder.build();
        }, responseObserver, request);
  }
}
