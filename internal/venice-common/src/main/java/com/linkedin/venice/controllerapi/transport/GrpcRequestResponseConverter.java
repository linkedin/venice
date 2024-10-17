package com.linkedin.venice.controllerapi.transport;

import com.google.protobuf.Any;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.request.ControllerRequest;
import com.linkedin.venice.protocols.controller.ClusterStoreGrpcInfo;
import com.linkedin.venice.protocols.controller.ControllerGrpcErrorType;
import com.linkedin.venice.protocols.controller.VeniceControllerGrpcErrorInfo;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.StreamObserver;


public class GrpcRequestResponseConverter {
  public static ClusterStoreGrpcInfo getClusterStoreGrpcInfo(ControllerResponse response) {
    ClusterStoreGrpcInfo.Builder builder = ClusterStoreGrpcInfo.newBuilder();
    if (response.getCluster() != null) {
      builder.setClusterName(response.getCluster());
    }
    if (response.getName() != null) {
      builder.setStoreName(response.getName());
    }
    return builder.build();
  }

  public static ClusterStoreGrpcInfo getClusterStoreGrpcInfo(ControllerRequest request) {
    ClusterStoreGrpcInfo.Builder builder = ClusterStoreGrpcInfo.newBuilder();
    if (request.getClusterName() != null) {
      builder.setClusterName(request.getClusterName());
    }
    if (request.getStoreName() != null) {
      builder.setStoreName(request.getStoreName());
    }
    return builder.build();
  }

  public static void sendErrorResponse(
      Code code,
      ControllerGrpcErrorType errorType,
      Exception e,
      String clusterName,
      String storeName,
      StreamObserver<?> responseObserver) {
    VeniceControllerGrpcErrorInfo.Builder errorInfoBuilder =
        VeniceControllerGrpcErrorInfo.newBuilder().setStatusCode(code.value()).setErrorType(errorType);
    if (e.getMessage() != null) {
      errorInfoBuilder.setErrorMessage(e.getMessage());
    }
    if (clusterName != null) {
      errorInfoBuilder.setClusterName(clusterName);
    }
    if (storeName != null) {
      errorInfoBuilder.setStoreName(storeName);
    }
    // Wrap the error info into a com.google.rpc.Status message
    com.google.rpc.Status status =
        com.google.rpc.Status.newBuilder().setCode(code.value()).addDetails(Any.pack(errorInfoBuilder.build())).build();

    // Send the error response
    responseObserver.onError(StatusProto.toStatusRuntimeException(status));
  }

  public static VeniceControllerGrpcErrorInfo parseControllerGrpcError(StatusRuntimeException e) {
    com.google.rpc.Status status = StatusProto.fromThrowable(e);
    if (status != null) {
      for (com.google.protobuf.Any detail: status.getDetailsList()) {
        if (detail.is(VeniceControllerGrpcErrorInfo.class)) {
          try {
            return detail.unpack(VeniceControllerGrpcErrorInfo.class);
          } catch (Exception unpackException) {
            throw new VeniceClientException("Failed to unpack error details: " + unpackException.getMessage());
          }
        }
      }
    }
    throw new VeniceClientException("An unknown gRPC error occurred. Error code: " + Code.UNKNOWN.name());
  }
}
