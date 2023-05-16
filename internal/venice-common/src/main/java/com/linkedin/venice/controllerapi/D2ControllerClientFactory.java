package com.linkedin.venice.controllerapi;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.SharedObjectFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;


public class D2ControllerClientFactory {
  private static final SharedObjectFactory<D2ControllerClient> SHARED_OBJECT_FACTORY = new SharedObjectFactory<>();
  private static final Map<ControllerClient, String> CONTROLLER_CLIENT_TO_IDENTIFIER_MAP = new HashMap<>();

  public static D2ControllerClient getControllerClient(
      String d2ServiceName,
      String clusterName,
      String d2ZkHost,
      Optional<SSLFactory> sslFactory) {
    final String clientIdentifier = clusterName + d2ServiceName + d2ZkHost;
    return getIfAbsent(
        clientIdentifier,
        () -> new D2ControllerClient(d2ServiceName, clusterName, d2ZkHost, sslFactory));
  }

  public static D2ControllerClient getControllerClient(String d2ServiceName, String clusterName, D2Client d2Client) {
    final String clientIdentifier = clusterName + d2ServiceName + d2Client.hashCode();
    return getIfAbsent(clientIdentifier, () -> new D2ControllerClient(d2ServiceName, clusterName, d2Client));
  }

  public static boolean release(D2ControllerClient client) {
    String clientIdentifier = CONTROLLER_CLIENT_TO_IDENTIFIER_MAP.get(client);
    if (clientIdentifier != null) {
      return SHARED_OBJECT_FACTORY.release(clientIdentifier);
    }
    return true;
  }

  public static D2ControllerClient discoverAndConstructControllerClient(
      String storeName,
      String d2ServiceName,
      int retryAttempts,
      D2Client d2Client) {
    D2ServiceDiscoveryResponse discoResponse =
        D2ControllerClient.discoverCluster(d2Client, d2ServiceName, storeName, retryAttempts);
    String clusterName = discoResponse.getCluster();
    return getControllerClient(d2ServiceName, clusterName, d2Client);
  }

  public static D2ControllerClient discoverAndConstructControllerClient(
      String storeName,
      String d2ServiceName,
      String d2ZkHost,
      Optional<SSLFactory> sslFactory,
      int retryAttempts) {
    D2ServiceDiscoveryResponse discoResponse =
        D2ControllerClient.discoverCluster(d2ZkHost, d2ServiceName, storeName, retryAttempts);
    String clusterName = discoResponse.getCluster();
    return getControllerClient(d2ServiceName, clusterName, d2ZkHost, sslFactory);
  }

  private static D2ControllerClient getIfAbsent(
      String clientIdentifier,
      Supplier<D2ControllerClient> d2ControllerClientSupplier) {
    return SHARED_OBJECT_FACTORY.get(clientIdentifier, () -> {
      D2ControllerClient client = d2ControllerClientSupplier.get();
      CONTROLLER_CLIENT_TO_IDENTIFIER_MAP.put(client, clientIdentifier);
      return client;
    }, client -> {
      CONTROLLER_CLIENT_TO_IDENTIFIER_MAP.remove(client, clientIdentifier);
      client.close(); // Doesn't run anything right now - but is useful to clean up if close method adds some cleanup
      // functionality later
    });
  }
}
