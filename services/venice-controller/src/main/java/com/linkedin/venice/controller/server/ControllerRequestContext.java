package com.linkedin.venice.controller.server;

import java.security.cert.X509Certificate;
import java.util.Optional;


/**
 * Transport-agnostic request context that carries client identity information.
 * Both HTTP and gRPC layers populate this before calling handlers.
 */
public class ControllerRequestContext {
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
}
