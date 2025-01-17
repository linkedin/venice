package com.linkedin.venice.controller.grpc.server.interceptor;

import com.linkedin.venice.controller.grpc.ControllerGrpcConstants;
import com.linkedin.venice.utils.LatencyUtils;
import io.grpc.ForwardingServerCall.SimpleForwardingServerCall;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import java.net.SocketAddress;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * A gRPC server interceptor for audit logging.
 *
 * <p>This interceptor logs incoming and outgoing gRPC calls, including the API method name,
 * server address, client address, cluster name, store name, and request latency. It is useful
 * for debugging and monitoring gRPC requests and responses.</p>
 *
 */
public class ControllerGrpcAuditLoggingInterceptor implements ServerInterceptor {
  private static final Logger LOGGER = LogManager.getLogger(ControllerGrpcAuditLoggingInterceptor.class);

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> serverCall,
      Metadata headers,
      ServerCallHandler<ReqT, RespT> next) {

    // Extract details for logging
    String apiName = serverCall.getMethodDescriptor().getBareMethodName();
    String serverAddr = getAddress(serverCall.getAttributes().get(Grpc.TRANSPORT_ATTR_LOCAL_ADDR));
    String clientAddr = getAddress(serverCall.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR));
    String clusterName = headers.get(ControllerGrpcConstants.CLUSTER_NAME_METADATA_KEY);
    String storeName = headers.get(ControllerGrpcConstants.STORE_NAME_METADATA_KEY);

    LOGGER.info(
        "[AUDIT][gRPC][IN] api={}, serverAddr={}, clientAddr={}, clusterName={}, storeName={}",
        apiName,
        serverAddr,
        clientAddr,
        clusterName,
        storeName);

    // Start time for latency calculation
    long startTime = System.currentTimeMillis();

    // Wrap the server call to log response status
    SimpleForwardingServerCall<ReqT, RespT> auditingServerCall =
        new SimpleForwardingServerCall<ReqT, RespT>(serverCall) {
          @Override
          public void close(Status status, Metadata trailers) {
            LOGGER.info(
                "[AUDIT][gRPC][OUT] api={}, serverAddr={}, clientAddr={}, clusterName={}, storeName={}, status={}, latencyMs={}",
                apiName,
                serverAddr,
                clientAddr,
                clusterName,
                storeName,
                status.getCode(),
                LatencyUtils.getElapsedTimeFromMsToMs(startTime));
            super.close(status, trailers);
          }
        };

    // Proceed with the call
    return next.startCall(auditingServerCall, headers);
  }

  private String getAddress(SocketAddress address) {
    return address != null ? address.toString() : ControllerGrpcConstants.UNKNOWN_REMOTE_ADDRESS;
  }
}
