package com.linkedin.venice.grpc;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.SslUtils;
import javax.net.ssl.KeyManager;
import javax.net.ssl.TrustManager;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


public class GrpcSslUtilsTest {
  private static SSLFactory sslFactory;

  @BeforeTest
  public static void setup() {
    sslFactory = SslUtils.getVeniceLocalSslFactory();
  }

  @Test
  public void testGetTrustManagers() throws Exception {
    TrustManager[] trustManagers = GrpcSslUtils.getTrustManagers(sslFactory);

    assertNotNull(trustManagers);
    assertTrue(trustManagers.length > 0);
  }

  @Test
  public void testGetKeyManagers() throws Exception {
    KeyManager[] keyManagers = GrpcSslUtils.getKeyManagers(sslFactory);

    assertNotNull(keyManagers);
    assertTrue(keyManagers.length > 0);
  }
}
