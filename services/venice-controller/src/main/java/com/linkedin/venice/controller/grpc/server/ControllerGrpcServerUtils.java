package com.linkedin.venice.controller.grpc.server;

import static com.linkedin.venice.controller.grpc.ControllerGrpcConstants.GRPC_CONTROLLER_CLIENT_DETAILS;

import com.linkedin.venice.controller.grpc.GrpcRequestResponseConverter;
import com.linkedin.venice.controller.server.VeniceControllerAccessManager;
import com.linkedin.venice.exceptions.VeniceUnauthorizedAccessException;
import com.linkedin.venice.protocols.controller.ClusterStoreGrpcInfo;
import com.linkedin.venice.protocols.controller.ControllerGrpcErrorType;
import io.grpc.Context;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ControllerGrpcServerUtils {
  private static final Logger LOGGER = LogManager.getLogger(ControllerGrpcServerUtils.class);

  @FunctionalInterface
  public interface GrpcRequestHandler<T> {
    T handle() throws Exception;
  }

  public static <T> void handleRequest(
      MethodDescriptor<?, ?> methodDescriptor,
      GrpcRequestHandler<T> handler,
      StreamObserver<T> responseObserver,
      ClusterStoreGrpcInfo storeGrpcInfo) {
    handleRequest(
        methodDescriptor,
        handler,
        responseObserver,
        storeGrpcInfo.getClusterName(),
        storeGrpcInfo.getStoreName());
  }

  public static <T> void handleRequest(
      MethodDescriptor<?, ?> methodDescriptor,
      GrpcRequestHandler<T> handler,
      StreamObserver<T> responseObserver,
      String clusterName,
      String storeName) {
    String methodName = methodDescriptor.getFullMethodName();
    try {
      LOGGER.info("Handling gRPC request for method: {} cluster: {}, store: {}", methodName, clusterName, storeName);
      responseObserver.onNext(handler.handle());
      responseObserver.onCompleted();
    } catch (IllegalArgumentException e) {
      LOGGER.error("Invalid argument for method: {} on cluster: {}, store: {}", methodName, clusterName, storeName, e);
      GrpcRequestResponseConverter.sendErrorResponse(
          Status.Code.INVALID_ARGUMENT,
          ControllerGrpcErrorType.BAD_REQUEST,
          e,
          clusterName,
          storeName,
          responseObserver);
    } catch (VeniceUnauthorizedAccessException e) {
      LOGGER
          .error("Unauthorized access for method: {} on cluster: {}, store: {}", methodName, clusterName, storeName, e);
      GrpcRequestResponseConverter.sendErrorResponse(
          Status.Code.PERMISSION_DENIED,
          ControllerGrpcErrorType.UNAUTHORIZED,
          e,
          clusterName,
          storeName,
          responseObserver);
    } catch (Exception e) {
      LOGGER.error("Error in method: {} on cluster: {}, store: {}", methodName, clusterName, storeName, e);
      GrpcRequestResponseConverter.sendErrorResponse(
          Status.Code.INTERNAL,
          ControllerGrpcErrorType.GENERAL_ERROR,
          e,
          clusterName,
          storeName,
          responseObserver);
    }
  }

  protected static GrpcControllerClientDetails getClientDetails(Context context) {
    GrpcControllerClientDetails clientDetails = GRPC_CONTROLLER_CLIENT_DETAILS.get(context);
    if (clientDetails == null) {
      clientDetails = GrpcControllerClientDetails.UNDEFINED_CLIENT_DETAILS;
    }
    return clientDetails;
  }

  public static boolean isAllowListUser(
      VeniceControllerAccessManager accessManager,
      String resourceName,
      Context context) {
    GrpcControllerClientDetails clientDetails = getClientDetails(context);
    return accessManager.isAllowListUser(resourceName, clientDetails.getClientCertificate());
  }
}
