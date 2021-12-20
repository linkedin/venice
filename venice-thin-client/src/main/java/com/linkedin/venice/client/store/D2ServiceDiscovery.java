package com.linkedin.venice.client.store;

import com.linkedin.venice.client.exceptions.ServiceDiscoveryException;
import com.linkedin.venice.client.store.transport.D2TransportClient;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponseV2;
import io.tehuti.utils.SystemTime;
import io.tehuti.utils.Time;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;

import static com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponseV2.*;


/**
 * This class is used to find the proper d2 service name for the given store through default D2 service
 * d2://VeniceRouter. Then build the transport client based on the d2 service it found.
 */
public class D2ServiceDiscovery {
  private static final Logger LOGGER = LogManager.getLogger(D2ServiceDiscovery.class);
  public static final String TYPE_D2_SERVICE_DISCOVERY = "discover_cluster";
  private final Time time = new SystemTime(); // TODO: Make it injectable if we need to control time via tests.

  public TransportClient getD2TransportClientForStore(D2TransportClient client, String storeName, boolean retryOnFailure) {
    D2ServiceDiscoveryResponseV2 d2ServiceDiscoveryResponse = discoverD2Service(client, storeName, retryOnFailure);
    client.setServiceName(d2ServiceDiscoveryResponse.getD2Service());
    return client;
  }

  public D2ServiceDiscoveryResponseV2 discoverD2Service(D2TransportClient client, String storeName) {
    return discoverD2Service(client, storeName, true);
  }

  public D2ServiceDiscoveryResponseV2 discoverD2Service(D2TransportClient client, String storeName, boolean retryOnFailure) {
    try {
      TransportClientResponse response = null;
      int currentAttempt = 0;
      final int MAX_ATTEMPT = retryOnFailure ? 10 : 1;
      final long SLEEP_TIME_BETWEEN_ATTEMPTS = 5 * Time.MS_PER_SECOND;
      while (response == null) {
        if (currentAttempt >= MAX_ATTEMPT) {
          throw new ServiceDiscoveryException("Failed to find d2 service for store " + storeName + " after " + MAX_ATTEMPT + " attempts.");
        }
        if (currentAttempt > 0) {
          // Back off
          LOGGER.warn("Failed to fetch from the service discovery endpoint. Attempt: " + currentAttempt + "/" + MAX_ATTEMPT
              + ". Will sleep " + SLEEP_TIME_BETWEEN_ATTEMPTS + " ms and retry.");
          time.sleep(SLEEP_TIME_BETWEEN_ATTEMPTS);
        }
        CompletableFuture<TransportClientResponse> responseFuture = client.get(
            TYPE_D2_SERVICE_DISCOVERY + "/" + storeName,
            Collections.singletonMap(D2_SERVICE_DISCOVERY_RESPONSE_V2_ENABLED, "true"));
        try {
          response = responseFuture.get();
        } catch (ExecutionException e) {
          LOGGER.warn("ExecutionException when trying to get the service discovery response", e);
          response = null;
        }
        currentAttempt++;
      }
      byte[] body = response.getBody();
      ObjectMapper mapper = new ObjectMapper()
          .configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
      D2ServiceDiscoveryResponseV2 d2ServiceDiscoveryResponse = mapper.readValue(body, D2ServiceDiscoveryResponseV2.class);
      if (d2ServiceDiscoveryResponse.isError()) {
        throw new ServiceDiscoveryException(
            "Failed to find d2 service for store: " + storeName + ". " + d2ServiceDiscoveryResponse.getError());
      }
      LOGGER.info("Found d2 service: " + d2ServiceDiscoveryResponse.getD2Service() + " for store: " + storeName);
      return d2ServiceDiscoveryResponse;
    } catch (Exception e) {
      throw new ServiceDiscoveryException("Failed to find d2 service for store: " + storeName, e);
    }
  }
}
