package com.linkedin.venice.grpc;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.acl.handler.AccessResult;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.SslUtils;
import io.grpc.Status;
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
    Status grpcStatus = GrpcUtils.accessResultToGrpcStatus(AccessResult.GRANTED);
    assertEquals(
        grpcStatus.getCode(),
        Status.OK.getCode(),
        "Mismatch in GRPC status for the http response status permission denied");
    assertEquals(
        AccessResult.GRANTED.getMessage(),
        grpcStatus.getDescription(),
        "Mismatch in error description for the mapped grpc status");

    grpcStatus = GrpcUtils.accessResultToGrpcStatus(AccessResult.FORBIDDEN);
    assertEquals(
        grpcStatus.getCode(),
        Status.PERMISSION_DENIED.getCode(),
        "Mismatch in GRPC status for the http response status permission denied");
    assertEquals(
        AccessResult.FORBIDDEN.getMessage(),
        grpcStatus.getDescription(),
        "Mismatch in error description for the mapped grpc status");

    grpcStatus = GrpcUtils.accessResultToGrpcStatus(AccessResult.UNAUTHORIZED);
    assertEquals(
        grpcStatus.getCode(),
        Status.PERMISSION_DENIED.getCode(),
        "Mismatch in GRPC status for the http response status unauthorized");
    assertEquals(
        AccessResult.UNAUTHORIZED.getMessage(),
        grpcStatus.getDescription(),
        "Mismatch in error description for the mapped grpc status");
  }
}
