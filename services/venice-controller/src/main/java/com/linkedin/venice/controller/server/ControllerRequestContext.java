package com.linkedin.venice.controller.server;

import static com.linkedin.venice.VeniceConstants.CONTROLLER_SSL_CERTIFICATE_ATTRIBUTE_NAME;
import static com.linkedin.venice.controller.grpc.ControllerGrpcConstants.GRPC_CONTROLLER_CLIENT_DETAILS;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.NAME;

import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.acl.NoOpDynamicAccessController;
import com.linkedin.venice.controller.grpc.server.GrpcControllerClientDetails;
import com.linkedin.venice.exceptions.VeniceException;
import io.grpc.Context;
import java.security.cert.X509Certificate;
import java.util.Optional;
import javax.servlet.http.HttpServletRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import spark.Request;


/**
 * Transport-agnostic request context that carries client identity information.
 * Both HTTP and gRPC layers populate this before calling handlers.
 */
public class ControllerRequestContext {
  private static final Logger LOGGER = LogManager.getLogger(ControllerRequestContext.class);
  private static final String USER_UNKNOWN = "USER_UNKNOWN";

  private final Optional<X509Certificate> clientCertificate;
  private final String clientPrincipalId;

  public ControllerRequestContext(X509Certificate clientCertificate, String clientPrincipalId) {
    this.clientCertificate = Optional.ofNullable(clientCertificate);
    this.clientPrincipalId = clientPrincipalId;
  }

  public Optional<X509Certificate> getClientCertificate() {
    return clientCertificate;
  }

  public String getClientPrincipalId() {
    return clientPrincipalId;
  }

  /**
   * Creates a context for unauthenticated/internal requests
   */
  public static ControllerRequestContext anonymous() {
    return new ControllerRequestContext(null, "anonymous");
  }

  /**
   * Builds a ControllerRequestContext from a gRPC context.
   * Extracts client certificate and client address for request tracking and potential ACL checks.
   *
   * @param context the gRPC context containing client details
   * @return a ControllerRequestContext with client information
   */
  public static ControllerRequestContext fromGrpcContext(Context context) {
    GrpcControllerClientDetails clientDetails = GRPC_CONTROLLER_CLIENT_DETAILS.get(context);
    if (clientDetails == null) {
      clientDetails = GrpcControllerClientDetails.UNDEFINED_CLIENT_DETAILS;
    }
    return new ControllerRequestContext(
        clientDetails.getClientCertificate(),
        clientDetails.getClientAddress() != null ? clientDetails.getClientAddress() : "anonymous");
  }

  /**
   * Builds a ControllerRequestContext from an HTTP request.
   * Extracts client certificate and principal ID for request tracking and potential ACL checks.
   *
   * @param request the HTTP request
   * @param sslEnabled whether SSL is enabled
   * @param accessController optional access controller for principal ID extraction
   * @return a ControllerRequestContext with client information
   */
  public static ControllerRequestContext fromHttpRequest(
      Request request,
      boolean sslEnabled,
      Optional<DynamicAccessController> accessController) {
    if (!sslEnabled) {
      return anonymous();
    }
    X509Certificate cert = extractCertificate(request);
    String principalId = extractPrincipalId(cert, accessController);
    return new ControllerRequestContext(cert, principalId);
  }

  /**
   * Helper to extract certificate from HTTP request.
   */
  private static X509Certificate extractCertificate(Request request) {
    HttpServletRequest rawRequest = request.raw();
    Object certificateObject = rawRequest.getAttribute(CONTROLLER_SSL_CERTIFICATE_ATTRIBUTE_NAME);
    if (certificateObject == null) {
      throw new VeniceException("Client request doesn't contain certificate for store: " + request.queryParams(NAME));
    }
    return ((X509Certificate[]) certificateObject)[0];
  }

  /**
   * Helper to extract principal ID from certificate.
   */
  private static String extractPrincipalId(X509Certificate cert, Optional<DynamicAccessController> accessController) {
    if (accessController.isPresent() && !(accessController.get() instanceof NoOpDynamicAccessController)) {
      try {
        return accessController.get().getPrincipalId(cert);
      } catch (Exception e) {
        LOGGER.error("Error when retrieving principal Id from request", e);
        return USER_UNKNOWN;
      }
    } else {
      return cert.getSubjectX500Principal().getName();
    }
  }
}
