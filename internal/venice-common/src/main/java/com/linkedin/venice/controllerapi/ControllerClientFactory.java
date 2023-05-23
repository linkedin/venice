package com.linkedin.venice.controllerapi;

import com.linkedin.venice.exceptions.ErrorType;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.exceptions.VeniceUnsupportedOperationException;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.SharedObjectFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;


public class ControllerClientFactory {
  // Visible for testing
  // Flag to denote if the test is in unit test mode and hence, will allow overriding the ControllerClient
  private static boolean unitTestMode = false;

  private static final SharedObjectFactory<ControllerClient> SHARED_OBJECT_FACTORY = new SharedObjectFactory<>();
  private static final Map<ControllerClient, String> CONTROLLER_CLIENT_TO_IDENTIFIER_MAP = new HashMap<>();

  static void setUnitTestMode() {
    unitTestMode = true;
  }

  static void resetUnitTestMode() {
    unitTestMode = false;
  }

  // Allow for overriding with mock D2Client for unit tests. The caller must release the object to prevent side-effects
  static void setControllerClient(String clusterName, String discoveryUrls, ControllerClient controllerClient) {
    if (!unitTestMode) {
      throw new VeniceUnsupportedOperationException("setControllerClient in non-unit-test-mode");
    }
    final String clientIdentifier = clusterName + discoveryUrls;
    SHARED_OBJECT_FACTORY.get(clientIdentifier, () -> controllerClient, controllerClient1 -> {});
  }

  public static ControllerClient getControllerClient(
      String clusterName,
      String discoveryUrls,
      Optional<SSLFactory> sslFactory) {
    final String clientIdentifier = clusterName + discoveryUrls;
    return createIfAbsent(clientIdentifier, () -> new ControllerClient(clusterName, discoveryUrls, sslFactory));
  }

  public static ControllerClient discoverAndConstructControllerClient(
      String storeName,
      String discoveryUrls,
      Optional<SSLFactory> sslFactory,
      int retryAttempts) {
    D2ServiceDiscoveryResponse discoveryResponse =
        ControllerClient.discoverCluster(discoveryUrls, storeName, sslFactory, retryAttempts);
    checkDiscoveryResponse(storeName, discoveryResponse);
    return getControllerClient(discoveryResponse.getCluster(), discoveryUrls, sslFactory);
  }

  public static boolean release(ControllerClient client) {
    String clientIdentifier = CONTROLLER_CLIENT_TO_IDENTIFIER_MAP.get(client);
    if (clientIdentifier != null) {
      return SHARED_OBJECT_FACTORY.release(clientIdentifier);
    }
    return true;
  }

  private static ControllerClient createIfAbsent(
      String clientIdentifier,
      Supplier<ControllerClient> controllerClientSupplier) {
    return SHARED_OBJECT_FACTORY.get(clientIdentifier, () -> {
      ControllerClient client = controllerClientSupplier.get();
      CONTROLLER_CLIENT_TO_IDENTIFIER_MAP.put(client, clientIdentifier);
      return client;
    }, client -> {
      CONTROLLER_CLIENT_TO_IDENTIFIER_MAP.remove(client, clientIdentifier);
      client.close(); // Doesn't run anything right now - but is useful to clean up if close method adds some cleanup
      // functionality later
    });
  }

  private static void checkDiscoveryResponse(String storeName, D2ServiceDiscoveryResponse discoveryResponse) {
    if (discoveryResponse.isError()) {
      if (ErrorType.STORE_NOT_FOUND.equals(discoveryResponse.getErrorType())) {
        throw new VeniceNoStoreException(storeName);
      } else {
        throw new VeniceException("Unable to discover cluster for store " + storeName + ". Check if it exists.");
      }
    }
  }
}
