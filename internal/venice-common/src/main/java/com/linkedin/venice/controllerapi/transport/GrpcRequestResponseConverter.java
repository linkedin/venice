package com.linkedin.venice.controllerapi.transport;

import com.google.rpc.ErrorInfo;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.request.ControllerRequest;
import com.linkedin.venice.controllerapi.request.NewStoreRequest;
import com.linkedin.venice.controllerapi.request.UpdateAclForStoreRequest;
import com.linkedin.venice.protocols.ClusterStoreGrpcInfo;
import com.linkedin.venice.protocols.CreateStoreGrpcRequest;
import com.linkedin.venice.protocols.CreateStoreGrpcResponse;
import com.linkedin.venice.protocols.UpdateAclForStoreGrpcRequest;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;


public class GrpcRequestResponseConverter {
  public static CreateStoreGrpcRequest toGrpcCreateStoreGrpcRequest(NewStoreRequest newStoreRequest) {
    CreateStoreGrpcRequest.Builder builder = CreateStoreGrpcRequest.newBuilder()
        .setClusterStoreInfo(
            ClusterStoreGrpcInfo.newBuilder()
                .setClusterName(newStoreRequest.getClusterName())
                .setStoreName(newStoreRequest.getStoreName())
                .build())
        .setKeySchema(newStoreRequest.getKeySchema())
        .setValueSchema(newStoreRequest.getValueSchema())
        .setIsSystemStore(newStoreRequest.isSystemStore());

    // For optional fields, only set if they are not null
    if (newStoreRequest.getOwner() != null) {
      builder.setOwner(newStoreRequest.getOwner());
    }
    if (newStoreRequest.getAccessPermissions() != null) {
      builder.setAccessPermission(newStoreRequest.getAccessPermissions());
    }
    return builder.build();
  }

  public static NewStoreResponse fromGrpcResponse(CreateStoreGrpcResponse grpcResponse) {
    NewStoreResponse response = new NewStoreResponse();
    response.setOwner(grpcResponse.getOwner());
    return response;
  }

  public static NewStoreRequest convertGrpcRequestToNewStoreRequest(CreateStoreGrpcRequest grpcRequest) {
    return new NewStoreRequest(
        grpcRequest.getClusterStoreInfo().getClusterName(),
        grpcRequest.getClusterStoreInfo().getStoreName(),
        grpcRequest.hasOwner() ? grpcRequest.getOwner() : null,
        grpcRequest.getKeySchema(),
        grpcRequest.getValueSchema(),
        grpcRequest.hasAccessPermission() ? grpcRequest.getAccessPermission() : null,
        grpcRequest.getIsSystemStore());
  }

  // UpdateAclForStoreRequest
  public static UpdateAclForStoreRequest convertGrpcRequestToUpdateAclForStoreRequest(
      UpdateAclForStoreGrpcRequest grpcRequest) {
    return new UpdateAclForStoreRequest(
        grpcRequest.getClusterStoreInfo().getClusterName(),
        grpcRequest.getClusterStoreInfo().getStoreName(),
        grpcRequest.getAccessPermissions());
  }

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

  public static ControllerRequest getControllerRequest(ClusterStoreGrpcInfo clusterStoreGrpcInfo) {
    return new ControllerRequest(clusterStoreGrpcInfo.getClusterName(), clusterStoreGrpcInfo.getStoreName());
  }

  /**
   * Handles the gRPC exception by extracting the error details and returning a VeniceException.
   *
   * @param e the gRPC StatusRuntimeException
   * @return a VeniceException with the extracted error details
   */
  public static VeniceClientException handleGrpcError(StatusRuntimeException e) {
    com.google.rpc.Status status = StatusProto.fromThrowable(e);

    if (status != null) {
      // Extract gRPC status code and message
      int errorCode = status.getCode();
      StringBuilder errorMessage = new StringBuilder(status.getMessage());

      // Process ErrorInfo if present in the details
      for (com.google.protobuf.Any detail: status.getDetailsList()) {
        if (detail.is(ErrorInfo.class)) {
          try {
            ErrorInfo errorInfo = detail.unpack(ErrorInfo.class);
            // Append the error info to the error message
            errorMessage.append(" Reason: ")
                .append(errorInfo.getReason())
                .append(", Metadata: ")
                .append(errorInfo.getMetadataMap());
          } catch (Exception unpackException) {
            // If unpacking fails, include that info in the message
            errorMessage.append(". Failed to unpack error details: ").append(unpackException.getMessage());
          }
        }
      }

      // Return a VeniceException with error code and message
      return new VeniceClientException(
          "gRPC error occurred. Error code: " + errorCode + ", Error message: " + errorMessage);
    }

    // Return a generic VeniceException if no detailed gRPC status is found
    return new VeniceClientException("An unknown gRPC error occurred. Error code: " + Code.UNKNOWN.name());
  }
}
