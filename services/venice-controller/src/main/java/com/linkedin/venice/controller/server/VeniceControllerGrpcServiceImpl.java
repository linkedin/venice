package com.linkedin.venice.controller.server;

import static com.linkedin.venice.controller.grpc.server.ControllerGrpcServerUtils.handleRequest;

import com.linkedin.venice.protocols.controller.DiscoverClusterGrpcRequest;
import com.linkedin.venice.protocols.controller.DiscoverClusterGrpcResponse;
import com.linkedin.venice.protocols.controller.LeaderControllerGrpcRequest;
import com.linkedin.venice.protocols.controller.LeaderControllerGrpcResponse;
import com.linkedin.venice.protocols.controller.VeniceControllerGrpcServiceGrpc;
import com.linkedin.venice.protocols.controller.VeniceControllerGrpcServiceGrpc.VeniceControllerGrpcServiceImplBase;
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

  @Override
  public void getLeaderController(
      LeaderControllerGrpcRequest request,
      StreamObserver<LeaderControllerGrpcResponse> responseObserver) {
    LOGGER.debug("Received getLeaderController with args: {}", request);
    handleRequest(
        VeniceControllerGrpcServiceGrpc.getGetLeaderControllerMethod(),
        () -> requestHandler.getLeaderControllerDetails(request),
        responseObserver,
        request.getClusterName(),
        null);
  }

  @Override
  public void discoverClusterForStore(
      DiscoverClusterGrpcRequest grpcRequest,
      StreamObserver<DiscoverClusterGrpcResponse> responseObserver) {
    LOGGER.debug("Received discoverClusterForStore with args: {}", grpcRequest);
    handleRequest(
        VeniceControllerGrpcServiceGrpc.getDiscoverClusterForStoreMethod(),
        () -> requestHandler.discoverCluster(grpcRequest),
        responseObserver,
        null,
        grpcRequest.getStoreName());
  }
}
