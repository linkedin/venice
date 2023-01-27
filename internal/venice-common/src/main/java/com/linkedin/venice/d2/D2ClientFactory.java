package com.linkedin.venice.d2;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.ReferenceCounted;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Map;
import java.util.Optional;


public class D2ClientFactory {
  // Visible for testing
  // Cache for reusing d2 clients for a zk cluster
  protected static final Map<String, ReferenceCounted<D2Client>> D2_CLIENT_CACHE = new VeniceConcurrentHashMap<>();

  public static ReferenceCounted<D2Client> getD2Client(String d2ZKHost, Optional<SSLFactory> sslFactoryOptional) {
    return getD2Client(d2ZKHost, sslFactoryOptional, false);
  }

  // Visible for testing
  // In unit tests, we do not spin up a Zk Server, so we test without starting the client.
  protected static ReferenceCounted<D2Client> getD2Client(
      String d2ZKHost,
      Optional<SSLFactory> sslFactoryOptional,
      boolean skipClientStart) {
    ReferenceCounted<D2Client> d2ClientRef;
    if (D2_CLIENT_CACHE.containsKey(d2ZKHost)) {
      d2ClientRef = D2_CLIENT_CACHE.get(d2ZKHost);
      d2ClientRef.retain();
    } else {
      D2Client d2Client = new D2ClientBuilder().setZkHosts(d2ZKHost)
          .setSSLContext(sslFactoryOptional.map(SSLFactory::getSSLContext).orElse(null))
          .setIsSSLEnabled(sslFactoryOptional.isPresent())
          .setSSLParameters(sslFactoryOptional.map(SSLFactory::getSSLParameters).orElse(null))
          .build();
      if (!skipClientStart) {
        D2ClientUtils.startClient(d2Client);
      }
      d2ClientRef = new ReferenceCounted<>(d2Client, client -> {
        D2_CLIENT_CACHE.remove(d2ZKHost);
        if (!skipClientStart) {
          D2ClientUtils.shutdownClient(client);
        }
      });
      D2_CLIENT_CACHE.put(d2ZKHost, d2ClientRef);
    }
    return d2ClientRef;
  }
}
