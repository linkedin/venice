package com.linkedin.venice.d2;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.exceptions.VeniceUnsupportedOperationException;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.SharedObjectFactory;
import java.util.Optional;


public class D2ClientFactory {
  // Visible for testing
  // Flag to denote if the test is in unit test mode and hence, will not start the D2Client
  private static boolean unitTestMode = false;
  // Cache for reusing d2 clients for a zk cluster
  protected static final SharedObjectFactory<D2Client> SHARED_OBJECT_FACTORY = new SharedObjectFactory<>();

  public static void setUnitTestMode() {
    unitTestMode = true;
  }

  public static void resetUnitTestMode() {
    unitTestMode = false;
  }

  // Allow for overriding with mock D2Client for unit tests. The caller must release the object to prevent side-effects
  public static void setD2Client(String d2ZkHost, D2Client d2Client) {
    if (!unitTestMode) {
      throw new VeniceUnsupportedOperationException("setD2Client in non-unit-test-mode");
    }

    SHARED_OBJECT_FACTORY.getObject(d2ZkHost, () -> d2Client, d2Client1 -> {});
  }

  // Visible for testing
  // In unit tests, we do not spin up a Zk Server, so we test without starting the client.
  public static D2Client getD2Client(String d2ZkHost, Optional<SSLFactory> sslFactoryOptional) {
    return SHARED_OBJECT_FACTORY.getObject(d2ZkHost, () -> {
      D2Client d2Client = new D2ClientBuilder().setZkHosts(d2ZkHost)
          .setSSLContext(sslFactoryOptional.map(SSLFactory::getSSLContext).orElse(null))
          .setIsSSLEnabled(sslFactoryOptional.isPresent())
          .setSSLParameters(sslFactoryOptional.map(SSLFactory::getSSLParameters).orElse(null))
          .build();
      if (!unitTestMode) {
        D2ClientUtils.startClient(d2Client);
      }
      return d2Client;
    }, d2Client -> {
      if (!unitTestMode) {
        D2ClientUtils.shutdownClient(d2Client);
      }
    });
  }

  public static void release(String d2ZkHost) {
    SHARED_OBJECT_FACTORY.release(d2ZkHost);
  }
}
