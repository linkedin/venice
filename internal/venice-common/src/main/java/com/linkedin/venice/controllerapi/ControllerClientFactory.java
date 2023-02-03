package com.linkedin.venice.controllerapi;

import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.SharedObjectFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;


public class ControllerClientFactory {
  private static final SharedObjectFactory<ControllerClient> SHARED_OBJECT_FACTORY = new SharedObjectFactory<>();
  private static final Map<ControllerClient, String> CONTROLLER_CLIENT_TO_IDENTIFIER_MAP = new HashMap<>();

  public static ControllerClient getControllerClient(
      String clusterName,
      String discoveryUrls,
      Optional<SSLFactory> sslFactory) {
    final String clientIdentifier = clusterName + discoveryUrls;
    return SHARED_OBJECT_FACTORY.get(clientIdentifier, () -> {
      ControllerClient client = new ControllerClient(clusterName, discoveryUrls, sslFactory);
      CONTROLLER_CLIENT_TO_IDENTIFIER_MAP.put(client, clientIdentifier);
      return client;
    }, client -> {
      CONTROLLER_CLIENT_TO_IDENTIFIER_MAP.remove(client, clientIdentifier);
      client.close(); // Doesn't run anything right now - but is useful to clean up if close method adds some cleanup
      // functionality later
    });
  }

  public static boolean release(ControllerClient client) {
    String clientIdentifier = CONTROLLER_CLIENT_TO_IDENTIFIER_MAP.get(client);
    if (clientIdentifier != null) {
      return SHARED_OBJECT_FACTORY.release(clientIdentifier);
    }
    return true;
  }
}
