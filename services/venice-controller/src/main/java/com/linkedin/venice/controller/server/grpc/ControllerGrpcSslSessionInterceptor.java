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


/**
 * Intercepts gRPC calls to enforce SSL/TLS requirements and propagate client certificate
 * and remote address details into the gRPC {@link Context}.
 *
 * <p>If the gRPC connection does not have an SSL session, the interceptor rejects the call
 * with an UNAUTHENTICATED status. Otherwise, it extracts the client certificate and remote
 * address, injecting them into the gRPC context for downstream processing.</p>
 *
 * <p>The following attributes are injected into the {@link Context}:
 * <ul>
 *   <li>{@code CLIENT_CERTIFICATE_CONTEXT_KEY}: The client's X.509 certificate.</li>
 *   <li>{@code CLIENT_ADDRESS_CONTEXT_KEY}: The client's remote address as a string.</li>
 * </ul>
 *
 * <p>Errors are logged if the SSL session is missing or if certificate extraction fails.</p>
 */
public class ControllerGrpcSslSessionInterceptor implements ServerInterceptor {
  private static final Logger LOGGER = LogManager.getLogger(ControllerGrpcSslSessionInterceptor.class);
  protected static final String UNKNOWN_REMOTE_ADDRESS = "unknown";

  public static final Context.Key<X509Certificate> CLIENT_CERTIFICATE_CONTEXT_KEY =
      Context.key("controller-client-certificate");
  public static final Context.Key<String> CLIENT_ADDRESS_CONTEXT_KEY = Context.key("controller-client-address");

  protected static final VeniceControllerGrpcErrorInfo NON_SSL_ERROR_INFO = VeniceControllerGrpcErrorInfo.newBuilder()
      .setStatusCode(Status.UNAUTHENTICATED.getCode().value())
      .setErrorType(ControllerGrpcErrorType.CONNECTION_ERROR)
      .setErrorMessage("SSL connection required")
      .build();

  protected static final StatusRuntimeException NON_SSL_CONNECTION_ERROR = StatusProto.toStatusRuntimeException(
      com.google.rpc.Status.newBuilder()
          .setCode(Status.UNAUTHENTICATED.getCode().value())
          .addDetails(com.google.protobuf.Any.pack(NON_SSL_ERROR_INFO))
          .build());

  /**
   * Intercepts a gRPC call to enforce SSL/TLS requirements and propagate SSL-related attributes
   * into the gRPC {@link Context}. This ensures that only secure connections with valid client
   * certificates proceed further in the call chain.
   *
   * <p>The method performs the following steps:
   * <ul>
   *   <li>Extracts the remote address from the server call attributes.</li>
   *   <li>Validates the presence of an SSL session. If absent, the call is closed with an
   *       {@code UNAUTHENTICATED} status.</li>
   *   <li>Attempts to extract the client certificate from the SSL session. If extraction fails,
   *       the call is closed with an {@code UNAUTHENTICATED} status.</li>
   *   <li>Creates a new {@link Context} containing the client certificate and remote address,
   *       and passes it to the downstream handlers.</li>
   * </ul>
   *
   * @param serverCall The gRPC server call being intercepted.
   * @param metadata The metadata associated with the call, containing headers and other request data.
   * @param serverCallHandler The downstream handler that processes the call if validation passes.
   * @param <ReqT> The request type of the gRPC method.
   * @param <RespT> The response type of the gRPC method.
   * @return A {@link ServerCall.Listener} for handling the intercepted call, or a no-op listener
   *         if the call is terminated early due to validation failure.
   */
  @Override
  public <ReqT, RespT> io.grpc.ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> serverCall,
      Metadata metadata,
      ServerCallHandler<ReqT, RespT> serverCallHandler) {

    // Extract remote address
    String remoteAddressStr = getRemoteAddress(serverCall);

    // Validate SSL session
    SSLSession sslSession = serverCall.getAttributes().get(Grpc.TRANSPORT_ATTR_SSL_SESSION);
    if (sslSession == null) {
      return closeWithSslError(serverCall);
    }

    // Extract client certificate
    X509Certificate clientCert = extractClientCertificate(serverCall);
    if (clientCert == null) {
      return closeWithSslError(serverCall);
    }

    // Create a new context with SSL-related attributes
    Context context = updateAndGetContext(clientCert, remoteAddressStr);

    // Proceed with the call
    return Contexts.interceptCall(context, serverCall, metadata, serverCallHandler);
  }

  /**
   * Retrieves the remote address from the server call attributes.
   *
   * @param serverCall The gRPC server call.
   * @return The remote address as a string, or "unknown" if not available.
   */
  private String getRemoteAddress(ServerCall<?, ?> serverCall) {
    SocketAddress remoteAddress = serverCall.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR);
    return remoteAddress != null ? remoteAddress.toString() : UNKNOWN_REMOTE_ADDRESS;
  }

  /**
   * Closes the server call with an SSL error status.
   *
   * @param serverCall The gRPC server call to close.
   * @param <ReqT> The request type.
   * @param <RespT> The response type.
   * @return A no-op listener to terminate the call.
   */
  private <ReqT, RespT> ServerCall.Listener<ReqT> closeWithSslError(ServerCall<ReqT, RespT> serverCall) {
    LOGGER.debug("SSL not enabled or client certificate extraction failed");
    serverCall.close(NON_SSL_CONNECTION_ERROR.getStatus(), NON_SSL_CONNECTION_ERROR.getTrailers());
    return new ServerCall.Listener<ReqT>() {
    };
  }

  /**
   * Extracts the client certificate from the gRPC server call.
   *
   * @param serverCall The gRPC server call.
   * @return The client certificate, or null if extraction fails.
   */
  private X509Certificate extractClientCertificate(ServerCall<?, ?> serverCall) {
    try {
      return GrpcUtils.extractGrpcClientCert(serverCall);
    } catch (Exception e) {
      LOGGER.error("Failed to extract client certificate", e);
      return null;
    }
  }

  /**
   * Updates the gRPC context of the current scope by adding SSL-related attributes.
   *
   * @param clientCert The client certificate.
   * @param remoteAddressStr The remote address as a string.
   * @return The new context.
   */
  private Context updateAndGetContext(X509Certificate clientCert, String remoteAddressStr) {
    return Context.current()
        .withValue(CLIENT_CERTIFICATE_CONTEXT_KEY, clientCert)
        .withValue(CLIENT_ADDRESS_CONTEXT_KEY, remoteAddressStr);
  }
}
