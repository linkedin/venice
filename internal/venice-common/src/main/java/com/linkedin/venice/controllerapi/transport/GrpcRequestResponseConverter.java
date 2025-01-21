package com.linkedin.venice.controllerapi.transport;

import com.google.protobuf.Any;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.controllerapi.ControllerResponse;
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

  /**
   * Sends an error response to the client using the provided error details.
   *
   * <p>This method constructs a detailed gRPC error response, including the error type, status code,
   * message, and optional cluster or store-specific information. The constructed error is sent
   * to the client via the provided {@link StreamObserver}.</p>
   *
   * @param code            The gRPC status code representing the error (e.g., {@link io.grpc.Status.Code}).
   * @param errorType       The specific controller error type represented by {@link ControllerGrpcErrorType}.
   * @param errorMessage    The error message to be included in the response.
   * @param clusterName     The name of the cluster associated with the error (can be null).
   * @param storeName       The name of the store associated with the error (can be null).
   * @param responseObserver The {@link StreamObserver} to send the error response back to the client.
   *
   * <p>Example usage:</p>
   * <pre>
   * {@code
   * sendErrorResponse(
   *     Status.Code.INTERNAL,
   *     ControllerGrpcErrorType.UNKNOWN_ERROR,
   *     new RuntimeException("Something went wrong"),
   *     "test-cluster",
   *     "test-store",
   *     responseObserver);
   * }
   * </pre>
   *
   * <p>The error response includes the following:</p>
   * <ul>
   *   <li>gRPC status code (e.g., INTERNAL, FAILED_PRECONDITION).</li>
   *   <li>Error type (e.g., BAD_REQUEST, CONCURRENT_BATCH_PUSH).</li>
   *   <li>Error message extracted from the provided exception.</li>
   *   <li>Optional cluster name and store name if provided.</li>
   * </ul>
   */
  public static void sendErrorResponse(
      Code code,
      ControllerGrpcErrorType errorType,
      String errorMessage,
      String clusterName,
      String storeName,
      StreamObserver<?> responseObserver) {
    VeniceControllerGrpcErrorInfo.Builder errorInfoBuilder =
        VeniceControllerGrpcErrorInfo.newBuilder().setStatusCode(code.value()).setErrorType(errorType);
    if (errorMessage != null) {
      errorInfoBuilder.setErrorMessage(errorMessage);
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

  public static void sendErrorResponse(
      Code code,
      ControllerGrpcErrorType errorType,
      Exception exception,
      String clusterName,
      String storeName,
      StreamObserver<?> responseObserver) {
    sendErrorResponse(
        code,
        errorType,
        exception != null ? exception.getMessage() : "",
        clusterName,
        storeName,
        responseObserver);
  }

  /**
   * Parses a {@link StatusRuntimeException} to extract a {@link VeniceControllerGrpcErrorInfo} object.
   *
   * <p>This method processes the gRPC error details embedded within a {@link StatusRuntimeException}.
   * If the error details contain a {@link VeniceControllerGrpcErrorInfo}, it unpacks and returns it.
   * If no valid details are found or the unpacking fails, a {@link VeniceClientException} is thrown.</p>
   *
   * @param e The {@link StatusRuntimeException} containing the gRPC error details.
   * @return A {@link VeniceControllerGrpcErrorInfo} object extracted from the error details.
   * @throws VeniceClientException If the error details cannot be unpacked or no valid information is found.
   *
   * <p>Example usage:</p>
   * <pre>
   * {@code
   * try {
   *     // Call a gRPC method that might throw StatusRuntimeException
   * } catch (StatusRuntimeException e) {
   *     VeniceControllerGrpcErrorInfo errorInfo = parseControllerGrpcError(e);
   *     System.out.println("Error Type: " + errorInfo.getErrorType());
   *     System.out.println("Error Message: " + errorInfo.getErrorMessage());
   * }
   * }
   * </pre>
   */
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
