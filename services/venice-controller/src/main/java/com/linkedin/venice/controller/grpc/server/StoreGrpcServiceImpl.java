package com.linkedin.venice.controller.grpc.server;

import static com.linkedin.venice.controller.grpc.server.ControllerGrpcServerUtils.handleRequest;
import static com.linkedin.venice.controller.grpc.server.ControllerGrpcServerUtils.isAllowListUser;
import static com.linkedin.venice.controller.server.VeniceRouteHandler.ACL_CHECK_FAILURE_WARN_MESSAGE_PREFIX;

import com.linkedin.venice.controller.server.StoreRequestHandler;
import com.linkedin.venice.controller.server.VeniceControllerAccessManager;
import com.linkedin.venice.exceptions.VeniceUnauthorizedAccessException;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.protocols.controller.ClusterStoreGrpcInfo;
import com.linkedin.venice.protocols.controller.CreateStoreGrpcRequest;
import com.linkedin.venice.protocols.controller.CreateStoreGrpcResponse;
import com.linkedin.venice.protocols.controller.DeleteAclForStoreGrpcRequest;
import com.linkedin.venice.protocols.controller.DeleteAclForStoreGrpcResponse;
import com.linkedin.venice.protocols.controller.GetAclForStoreGrpcRequest;
import com.linkedin.venice.protocols.controller.GetAclForStoreGrpcResponse;
import com.linkedin.venice.protocols.controller.GetStoreGrpcRequest;
import com.linkedin.venice.protocols.controller.GetStoreGrpcResponse;
import com.linkedin.venice.protocols.controller.ListStoresGrpcRequest;
import com.linkedin.venice.protocols.controller.ListStoresGrpcResponse;
import com.linkedin.venice.protocols.controller.ResourceCleanupCheckGrpcResponse;
import com.linkedin.venice.protocols.controller.StoreGrpcServiceGrpc;
import com.linkedin.venice.protocols.controller.StoreGrpcServiceGrpc.StoreGrpcServiceImplBase;
import com.linkedin.venice.protocols.controller.UpdateAclForStoreGrpcRequest;
import com.linkedin.venice.protocols.controller.UpdateAclForStoreGrpcResponse;
import com.linkedin.venice.protocols.controller.ValidateStoreDeletedGrpcRequest;
import com.linkedin.venice.protocols.controller.ValidateStoreDeletedGrpcResponse;
import com.linkedin.venice.utils.ObjectMapperFactory;
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

  @Override
  public void validateStoreDeleted(
      ValidateStoreDeletedGrpcRequest grpcRequest,
      StreamObserver<ValidateStoreDeletedGrpcResponse> responseObserver) {
    LOGGER.debug("Received validateStoreDeleted with args: {}", grpcRequest);
    String clusterName = grpcRequest.getStoreInfo().getClusterName();
    String storeName = grpcRequest.getStoreInfo().getStoreName();
    handleRequest(StoreGrpcServiceGrpc.getValidateStoreDeletedMethod(), () -> {
      if (!isAllowListUser(accessManager, storeName, Context.current())) {
        throw new VeniceUnauthorizedAccessException(
            ACL_CHECK_FAILURE_WARN_MESSAGE_PREFIX
                + StoreGrpcServiceGrpc.getValidateStoreDeletedMethod().getFullMethodName() + " on resource: "
                + storeName);
      }
      return storeRequestHandler.validateStoreDeleted(grpcRequest);
    }, responseObserver, clusterName, storeName);
  }

  /**
   * Lists all stores in a cluster with optional filtering.
   * No ACL check; any user can list stores.
   */
  @Override
  public void listStores(ListStoresGrpcRequest grpcRequest, StreamObserver<ListStoresGrpcResponse> responseObserver) {
    LOGGER.debug("Received listStores with args: {}", grpcRequest);
    String clusterName = grpcRequest.getClusterName();
    handleRequest(
        StoreGrpcServiceGrpc.getListStoresMethod(),
        () -> storeRequestHandler.listStores(grpcRequest),
        responseObserver,
        clusterName,
        null);
  }

  /**
   * Gets store information for a given store in a cluster.
   * No ACL check; this is a read-only operation.
   */
  @Override
  public void getStore(GetStoreGrpcRequest grpcRequest, StreamObserver<GetStoreGrpcResponse> responseObserver) {
    LOGGER.debug("Received getStore with args: {}", grpcRequest);
    String clusterName = grpcRequest.getStoreInfo().getClusterName();
    String storeName = grpcRequest.getStoreInfo().getStoreName();

    handleRequest(StoreGrpcServiceGrpc.getGetStoreMethod(), () -> {
      // Call handler with primitives
      StoreInfo storeInfo = storeRequestHandler.getStore(clusterName, storeName);

      // Convert POJO to protobuf response
      String storeInfoJson;
      try {
        storeInfoJson = ObjectMapperFactory.getInstance().writeValueAsString(storeInfo);
      } catch (Exception e) {
        throw new RuntimeException("Failed to serialize StoreInfo to JSON", e);
      }

      ClusterStoreGrpcInfo storeGrpcInfo =
          ClusterStoreGrpcInfo.newBuilder().setClusterName(clusterName).setStoreName(storeName).build();
      return GetStoreGrpcResponse.newBuilder().setStoreInfo(storeGrpcInfo).setStoreInfoJson(storeInfoJson).build();
    }, responseObserver, clusterName, storeName);
  }
}
