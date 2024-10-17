package com.linkedin.venice.controller.server.grpc;

import static com.linkedin.venice.controller.ParentControllerRegionState.ACTIVE;
import static com.linkedin.venice.controller.server.VeniceParentControllerRegionStateHandler.ACTIVE_CHECK_FAILURE_WARN_MESSAGE_PREFIX;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.ParentControllerRegionState;
import com.linkedin.venice.protocols.controller.ControllerGrpcErrorType;
import com.linkedin.venice.protocols.controller.VeniceControllerGrpcErrorInfo;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Interceptor to verify that the parent controller is active before processing requests within its region.
 */
public class ParentControllerRegionValidationInterceptor implements ServerInterceptor {
  private static final Logger LOGGER = LogManager.getLogger(ParentControllerRegionValidationInterceptor.class);
  private static final VeniceControllerGrpcErrorInfo.Builder ERROR_INFO_BUILDER =
      VeniceControllerGrpcErrorInfo.newBuilder()
          .setErrorType(ControllerGrpcErrorType.INCORRECT_CONTROLLER)
          .setStatusCode(Status.FAILED_PRECONDITION.getCode().value());

  private static final com.google.rpc.Status.Builder RPC_STATUS_BUILDER = com.google.rpc.Status.newBuilder()
      .setCode(Status.FAILED_PRECONDITION.getCode().value())
      .setMessage("Parent controller is not active");

  private final Admin admin;

  public ParentControllerRegionValidationInterceptor(Admin admin) {
    this.admin = admin;
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call,
      Metadata headers,
      ServerCallHandler<ReqT, RespT> next) {
    ParentControllerRegionState parentControllerRegionState = admin.getParentControllerRegionState();
    boolean isParent = admin.isParent();
    if (isParent && parentControllerRegionState != ACTIVE) {
      LOGGER.debug(
          "Parent controller is not active. Rejecting the request: {} from source: {}",
          call.getMethodDescriptor().getFullMethodName(),
          call.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR));
      // Retrieve the full method name
      String fullMethodName = call.getMethodDescriptor().getFullMethodName();
      VeniceControllerGrpcErrorInfo errorInfo =
          ERROR_INFO_BUILDER.setErrorMessage(ACTIVE_CHECK_FAILURE_WARN_MESSAGE_PREFIX + ": " + fullMethodName).build();
      // Note: On client side convert FAILED_PRECONDITION to SC_MISDIRECTED_REQUEST
      com.google.rpc.Status rpcStatus = RPC_STATUS_BUILDER.addDetails(com.google.protobuf.Any.pack(errorInfo)).build();
      StatusRuntimeException exception = StatusProto.toStatusRuntimeException(rpcStatus);
      call.close(exception.getStatus(), exception.getTrailers());
      return new ServerCall.Listener<ReqT>() {
      };
    }
    return next.startCall(call, headers);
  }
}
