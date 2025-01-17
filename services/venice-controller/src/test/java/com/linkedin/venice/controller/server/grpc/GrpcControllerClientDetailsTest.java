package com.linkedin.venice.controller.server.grpc;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import org.testng.annotations.Test;


public class GrpcControllerClientDetailsTest {
  @Test
  public void testGetClientAddress() {
    String clientAddress = "localhost";
    GrpcControllerClientDetails grpcControllerClientDetails = new GrpcControllerClientDetails(null, clientAddress);
    assertEquals(grpcControllerClientDetails.getClientAddress(), clientAddress);
    assertNull(grpcControllerClientDetails.getClientCertificate(), "Client certificate should be null");

    grpcControllerClientDetails = new GrpcControllerClientDetails(null, null);
    assertNull(grpcControllerClientDetails.getClientAddress());
    assertNull(grpcControllerClientDetails.getClientCertificate());
  }
}
