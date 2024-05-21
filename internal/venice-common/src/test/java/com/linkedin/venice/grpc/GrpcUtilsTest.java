package com.linkedin.venice.grpc;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.SslUtils;
import io.grpc.Status;
import io.netty.handler.codec.http.HttpResponseStatus;
import javax.net.ssl.KeyManager;
import javax.net.ssl.TrustManager;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


public class GrpcUtilsTest {
  private static SSLFactory sslFactory;

  @BeforeTest
  public static void setup() {
    sslFactory = SslUtils.getVeniceLocalSslFactory();
  }

  @Test
  public void testGetTrustManagers() throws Exception {
    TrustManager[] trustManagers = GrpcUtils.getTrustManagers(sslFactory);

    assertNotNull(trustManagers);
    assertTrue(trustManagers.length > 0);
  }

  @Test
  public void testGetKeyManagers() throws Exception {
    KeyManager[] keyManagers = GrpcUtils.getKeyManagers(sslFactory);

    assertNotNull(keyManagers);
    assertTrue(keyManagers.length > 0);
  }

  @Test
  public void testHttpResponseStatusToGrpcStatus() {
    final String permissionDeniedErrorMessage = "permission denied error message";
    Status grpcStatus =
        GrpcUtils.httpResponseStatusToGrpcStatus(HttpResponseStatus.FORBIDDEN, permissionDeniedErrorMessage);

    assertEquals(
        grpcStatus.getCode(),
        Status.PERMISSION_DENIED.getCode(),
        "Mismatch in GRPC status for the http response status permission denied");
    assertEquals(
        permissionDeniedErrorMessage,
        grpcStatus.getDescription(),
        "Mismatch in error description for the mapped grpc status");

    final String unauthorizedErrorMessage = "unauthorized error message";
    grpcStatus = GrpcUtils.httpResponseStatusToGrpcStatus(HttpResponseStatus.UNAUTHORIZED, unauthorizedErrorMessage);
    assertEquals(
        grpcStatus.getCode(),
        Status.PERMISSION_DENIED.getCode(),
        "Mismatch in GRPC status for the http response status unauthorized");
    assertEquals(
        unauthorizedErrorMessage,
        grpcStatus.getDescription(),
        "Mismatch in error description for the mapped grpc status");

    final String badRequestErrorMessage = "bad request error message";
    grpcStatus = GrpcUtils.httpResponseStatusToGrpcStatus(HttpResponseStatus.BAD_REQUEST, badRequestErrorMessage);
    assertEquals(grpcStatus.getCode(), Status.UNKNOWN.getCode(), "Expected unknown status for everything else");
    assertEquals(
        badRequestErrorMessage,
        grpcStatus.getDescription(),
        "Mismatch in error description for the mapped grpc status");
  }
}
