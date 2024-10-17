package com.linkedin.venice.controller.server;

import static com.linkedin.venice.controllerapi.transport.GrpcRequestResponseConverter.getClusterStoreGrpcInfo;

import com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponse;
import com.linkedin.venice.controllerapi.LeaderControllerResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.request.ClusterDiscoveryRequest;
import com.linkedin.venice.controllerapi.request.ControllerRequest;
import com.linkedin.venice.controllerapi.request.CreateNewStoreRequest;
import com.linkedin.venice.controllerapi.transport.GrpcRequestResponseConverter;
import com.linkedin.venice.protocols.controller.ControllerGrpcErrorType;
import com.linkedin.venice.protocols.controller.CreateStoreGrpcRequest;
import com.linkedin.venice.protocols.controller.CreateStoreGrpcResponse;
import com.linkedin.venice.protocols.controller.DiscoverClusterGrpcRequest;
import com.linkedin.venice.protocols.controller.DiscoverClusterGrpcResponse;
import com.linkedin.venice.protocols.controller.LeaderControllerGrpcRequest;
import com.linkedin.venice.protocols.controller.LeaderControllerGrpcResponse;
import com.linkedin.venice.protocols.controller.VeniceControllerGrpcServiceGrpc.VeniceControllerGrpcServiceImplBase;
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

  public VeniceControllerGrpcServiceImpl(VeniceControllerRequestHandler requestHandler) {
    this.requestHandler = requestHandler;
  }

  @Override
  public void getLeaderController(
      LeaderControllerGrpcRequest request,
      StreamObserver<LeaderControllerGrpcResponse> responseObserver) {
    String clusterName = request.getClusterName();
    LOGGER.info("Received gRPC request to get leader controller for cluster: {}", clusterName);
    try {
      LeaderControllerResponse response = new LeaderControllerResponse();
      ControllerRequest controllerRequest = new ControllerRequest(clusterName);
      requestHandler.getLeaderController(controllerRequest, response);
      LeaderControllerGrpcResponse.Builder grpcResponseBuilder =
          LeaderControllerGrpcResponse.newBuilder().setClusterName(response.getCluster()).setHttpUrl(response.getUrl());

      if (response.getSecureUrl() != null) {
        grpcResponseBuilder.setHttpsUrl(response.getSecureUrl());
      }
      if (response.getGrpcUrl() != null) {
        grpcResponseBuilder.setGrpcUrl(response.getGrpcUrl());
      }
      if (response.getSecureGrpcUrl() != null) {
        grpcResponseBuilder.setSecureGrpcUrl(response.getSecureGrpcUrl());
      }
      responseObserver.onNext(grpcResponseBuilder.build());
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
      D2ServiceDiscoveryResponse response = new D2ServiceDiscoveryResponse();
      requestHandler.discoverCluster(new ClusterDiscoveryRequest(grpcRequest.getStoreName()), response);
      DiscoverClusterGrpcResponse.Builder responseBuilder =
          DiscoverClusterGrpcResponse.newBuilder().setStoreName(response.getName());
      if (response.getCluster() != null) {
        responseBuilder.setClusterName(response.getCluster());
      }
      if (response.getD2Service() != null) {
        responseBuilder.setD2Service(response.getD2Service());
      }
      if (response.getServerD2Service() != null) {
        responseBuilder.setServerD2Service(response.getServerD2Service());
      }
      if (response.getZkAddress() != null) {
        responseBuilder.setZkAddress(response.getZkAddress());
      }
      if (response.getKafkaBootstrapServers() != null) {
        responseBuilder.setPubSubBootstrapServers(response.getKafkaBootstrapServers());
      }
      responseObserver.onNext(responseBuilder.build());
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
      // TODO (sushantmane) : Add the ACL check for allowlist users here

      // Convert the gRPC request to the internal request object
      CreateNewStoreRequest request = new CreateNewStoreRequest(
          grpcRequest.getClusterStoreInfo().getClusterName(),
          grpcRequest.getClusterStoreInfo().getStoreName(),
          grpcRequest.hasOwner() ? grpcRequest.getOwner() : null,
          grpcRequest.getKeySchema(),
          grpcRequest.getValueSchema(),
          grpcRequest.hasAccessPermission() ? grpcRequest.getAccessPermission() : null,
          grpcRequest.getIsSystemStore());

      // Create the store using the internal request object
      NewStoreResponse response = new NewStoreResponse();
      requestHandler.createStore(request, response);

      // Convert the internal response object to the gRPC response object and send the gRPC response
      responseObserver.onNext(
          CreateStoreGrpcResponse.newBuilder()
              .setClusterStoreInfo(getClusterStoreGrpcInfo(response))
              .setOwner(response.getOwner())
              .build());
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
