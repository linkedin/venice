package com.linkedin.venice.controller.server.grpc;

import java.security.cert.X509Certificate;


/**
 * Represents the details of a gRPC controller client, including the client's certificate and
 * address. This class is immutable and provides methods to access the client's X.509
 * certificate and address.
 *
 * <p>The primary purpose of this class is to encapsulate client-specific details passed
 * in a gRPC context, which can be utilized for authorization, auditing, or debugging.</p>
 */
public class GrpcControllerClientDetails {
  public static final GrpcControllerClientDetails UNDEFINED_CLIENT_DETAILS = new GrpcControllerClientDetails();

  private final String clientAddress;
  private final X509Certificate clientCertificate;

  private GrpcControllerClientDetails() {
    this.clientAddress = null;
    this.clientCertificate = null;
  }

  public GrpcControllerClientDetails(X509Certificate clientCertificate, String clientAddress) {
    this.clientAddress = clientAddress;
    this.clientCertificate = clientCertificate;
  }

  public String getClientAddress() {
    return clientAddress;
  }

  public X509Certificate getClientCertificate() {
    return clientCertificate;
  }
}
