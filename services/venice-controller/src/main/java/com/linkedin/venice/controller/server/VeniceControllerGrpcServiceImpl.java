package com.linkedin.venice.controller.server;

import static com.linkedin.venice.controller.grpc.ControllerGrpcConstants.GRPC_CONTROLLER_CLIENT_DETAILS;
import static com.linkedin.venice.controller.grpc.server.ControllerGrpcServerUtils.handleRequest;

import com.linkedin.venice.controller.grpc.server.ControllerGrpcServerUtils;
import com.linkedin.venice.controller.grpc.server.GrpcControllerClientDetails;
import com.linkedin.venice.controllerapi.MultiNodesStatusResponse;
import com.linkedin.venice.protocols.controller.ClusterHealthInstancesGrpcRequest;
import com.linkedin.venice.protocols.controller.ClusterHealthInstancesGrpcResponse;
import com.linkedin.venice.protocols.controller.DiscoverClusterGrpcRequest;
import com.linkedin.venice.protocols.controller.DiscoverClusterGrpcResponse;
import com.linkedin.venice.protocols.controller.LeaderControllerGrpcRequest;
import com.linkedin.venice.protocols.controller.LeaderControllerGrpcResponse;
import com.linkedin.venice.protocols.controller.VeniceControllerGrpcServiceGrpc;
import com.linkedin.venice.protocols.controller.VeniceControllerGrpcServiceGrpc.VeniceControllerGrpcServiceImplBase;
import io.grpc.Context;
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

  /**
   * Gets the health status of all storage instances in a cluster.
   * No ACL check; any user can get cluster health instances.
   */
  @Override
  public void getClusterHealthInstances(
      ClusterHealthInstancesGrpcRequest grpcRequest,
      StreamObserver<ClusterHealthInstancesGrpcResponse> responseObserver) {
    LOGGER.debug("Received getClusterHealthInstances with args: {}", grpcRequest);

    ControllerGrpcServerUtils
        .handleRequest(VeniceControllerGrpcServiceGrpc.getGetClusterHealthInstancesMethod(), () -> {
          // Extract primitives from protobuf
          String clusterName = grpcRequest.getClusterName();
          boolean enableDisabledReplicas =
              grpcRequest.hasEnableDisabledReplicas() && grpcRequest.getEnableDisabledReplicas();

          // Build transport-agnostic context from gRPC
          ControllerRequestContext context = buildRequestContext(Context.current());

          // Call handler - returns POJO
          MultiNodesStatusResponse result =
              requestHandler.getClusterHealthInstances(clusterName, enableDisabledReplicas, context);

          // Convert POJO to protobuf response
          return ClusterHealthInstancesGrpcResponse.newBuilder()
              .setClusterName(result.getCluster())
              .putAllInstancesStatusMap(result.getInstancesStatusMap())
              .build();
        }, responseObserver, grpcRequest.getClusterName(), null);
  }

  /**
   * Builds a ControllerRequestContext from the gRPC context.
   */
  private ControllerRequestContext buildRequestContext(Context context) {
    GrpcControllerClientDetails clientDetails = GRPC_CONTROLLER_CLIENT_DETAILS.get(context);
    if (clientDetails == null) {
      clientDetails = GrpcControllerClientDetails.UNDEFINED_CLIENT_DETAILS;
    }
    return new ControllerRequestContext(
        clientDetails.getClientCertificate(),
        clientDetails.getClientAddress() != null ? clientDetails.getClientAddress() : "anonymous");
  }
}
