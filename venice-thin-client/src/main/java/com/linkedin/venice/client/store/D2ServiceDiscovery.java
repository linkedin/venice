package com.linkedin.venice.client.store;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.transport.D2TransportClient;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponse;
import com.linkedin.venice.exceptions.VeniceException;
import io.tehuti.utils.SystemTime;
import io.tehuti.utils.Time;
import java.util.concurrent.CompletableFuture;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;


/**
 * This class is used to find the proper d2 service name for the given store through default D2 service
 * d2://VeniceRouter. Then build the transport client based on the d2 service it found.
 */
public class D2ServiceDiscovery {
  private static final Logger LOGGER = Logger.getLogger(D2ServiceDiscovery.class);
  public static final String TYPE_D2_SERVICE_DISCOVERY = "discover_cluster";
  private final Time time = new SystemTime(); // TODO: Make it injectable if we need to control time via tests.

  public TransportClient getD2TransportClientForStore(D2TransportClient client, String storeName) {
    String d2ServiceNameForStore = discoverD2Service(client, storeName);
    client.setServiceName(d2ServiceNameForStore);
    return client;
  }

  protected String discoverD2Service(D2TransportClient client, String storeName) {
    try {
      TransportClientResponse response = null;
      int currentAttempt = 0;
      final int MAX_ATTEMPT = 10;
      final long SLEEP_TIME_BETWEEN_ATTEMPTS = 5 * Time.MS_PER_SECOND;
      while (response == null) {
        if (currentAttempt >= MAX_ATTEMPT) {
          throw new VeniceException("Could not fetch from the service discovery endpoint after " + MAX_ATTEMPT + " attempts.");
        }
        if (currentAttempt > 0) {
          // Back off
          LOGGER.warn("Failed to fetch from the service discovery endpoint. Attempt: " + currentAttempt + "/" + MAX_ATTEMPT
              + ". Will sleep " + SLEEP_TIME_BETWEEN_ATTEMPTS + " ms and retry.");
          time.sleep(SLEEP_TIME_BETWEEN_ATTEMPTS);
        }
        CompletableFuture<TransportClientResponse> responseFuture = client.get(TYPE_D2_SERVICE_DISCOVERY + "/" + storeName);
        response = responseFuture.get();
        currentAttempt++;
      }
      byte[] body = response.getBody();
      ObjectMapper mapper = new ObjectMapper();
      D2ServiceDiscoveryResponse d2ServiceDiscoveryResponse = mapper.readValue(body, D2ServiceDiscoveryResponse.class);
      if (d2ServiceDiscoveryResponse.isError()) {
        throw new VeniceClientException(
            "Could not found d2 service for store: " + storeName + ". " + d2ServiceDiscoveryResponse.getError());
      }
      String d2Service = d2ServiceDiscoveryResponse.getD2Service();
      LOGGER.info("Found d2 service: " + d2Service + " for store: " + storeName);
      return d2Service;
    } catch (Exception e) {
      throw new VeniceClientException("Could not found d2 service for store: " + storeName, e);
    }
  }
}
