package com.linkedin.venice.controller.server.grpc;

import com.linkedin.venice.grpc.GrpcUtils;
import com.linkedin.venice.protocols.controller.ControllerGrpcErrorType;
import com.linkedin.venice.protocols.controller.VeniceControllerGrpcErrorInfo;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import java.net.SocketAddress;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLSession;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ControllerSslSessionInterceptor implements ServerInterceptor {
  private static final Logger LOGGER = LogManager.getLogger(ControllerSslSessionInterceptor.class);

  public static final Context.Key<X509Certificate> CLIENT_CERTIFICATE_CONTEXT_KEY =
      Context.key("controller-client-certificate");
  public static final Context.Key<String> CLIENT_ADDRESS_CONTEXT_KEY = Context.key("controller-client-address");

  private static final VeniceControllerGrpcErrorInfo NON_SSL_ERROR_INFO = VeniceControllerGrpcErrorInfo.newBuilder()
      .setStatusCode(Status.UNAUTHENTICATED.getCode().value())
      .setErrorType(ControllerGrpcErrorType.CONNECTION_ERROR)
      .setErrorMessage("SSL connection required")
      .build();

  private static final StatusRuntimeException NON_SSL_CONNECTION_STATUS = StatusProto.toStatusRuntimeException(
      com.google.rpc.Status.newBuilder()
          .setCode(Status.UNAUTHENTICATED.getCode().value())
          .addDetails(com.google.protobuf.Any.pack(NON_SSL_ERROR_INFO))
          .build());

  @Override
  public <ReqT, RespT> io.grpc.ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> serverCall,
      Metadata metadata,
      ServerCallHandler<ReqT, RespT> serverCallHandler) {
    SocketAddress remoteAddress = serverCall.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR);
    String remoteAddressStr = remoteAddress != null ? remoteAddress.toString() : "unknown";
    SSLSession sslSession = serverCall.getAttributes().get(Grpc.TRANSPORT_ATTR_SSL_SESSION);
    if (sslSession == null) {
      LOGGER.debug("SSL not enabled");
      serverCall.close(NON_SSL_CONNECTION_STATUS.getStatus(), NON_SSL_CONNECTION_STATUS.getTrailers());
      return new ServerCall.Listener<ReqT>() {
      };
    }

    X509Certificate clientCert;
    try {
      clientCert = GrpcUtils.extractGrpcClientCert(serverCall);
    } catch (Exception e) {
      LOGGER.error("Failed to extract client certificate", e);
      serverCall.close(NON_SSL_CONNECTION_STATUS.getStatus(), NON_SSL_CONNECTION_STATUS.getTrailers());
      return new ServerCall.Listener<ReqT>() {
      };
    }
    Context context = Context.current()
        .withValue(CLIENT_CERTIFICATE_CONTEXT_KEY, clientCert)
        .withValue(CLIENT_ADDRESS_CONTEXT_KEY, remoteAddressStr);
    return Contexts.interceptCall(context, serverCall, metadata, serverCallHandler);
  }
}
