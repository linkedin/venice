package com.linkedin.venice.client.store;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.transport.D2TransportClient;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponse;
import java.util.concurrent.CompletableFuture;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;


/**
 * This class is used to find the proper d2 service name for the given store through default D2 service
 * d2://VeniceRouter. Then build the transport client based on the d2 service it found.
 */
public class D2ServiceDiscoveryUtils {
  private static final Logger LOGGER = Logger.getLogger(D2ServiceDiscoveryUtils.class);
  public static final String TYPE_D2_SERVICE_DISCOVERY = "discover_cluster";

  public static TransportClient getD2TransportClientForStore(D2TransportClient client, String storeName) {
    String d2ServiceNameForStore = discoverD2Service(client, storeName);
    return new D2TransportClient(d2ServiceNameForStore, client.getD2Client());
  }

  protected static String discoverD2Service(D2TransportClient client, String storeName) {
    try {
      CompletableFuture<TransportClientResponse> response = client.get(TYPE_D2_SERVICE_DISCOVERY + "/" + storeName);
      byte[] body = response.get().getBody();
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
