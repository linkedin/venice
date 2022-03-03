package com.linkedin.venice.client.store;

import com.linkedin.venice.client.exceptions.ServiceDiscoveryException;
import com.linkedin.venice.client.store.transport.D2TransportClient;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponseV2;
import com.linkedin.venice.exceptions.ExceptionType;
import com.linkedin.venice.exceptions.VeniceException;

import com.linkedin.venice.exceptions.VeniceNoStoreException;
import java.util.Optional;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponseV2.*;


/**
 * This class is used to find the proper d2 service name for the given store through default D2 service
 * d2://VeniceRouter. Then build the transport client based on the d2 service it found.
 */
public class D2ServiceDiscovery {
  private static final Logger LOGGER = LogManager.getLogger(D2ServiceDiscovery.class);

  private static final ObjectMapper OBJECT_MAPPER =
      new ObjectMapper().disable(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES);

  public static final String TYPE_D2_SERVICE_DISCOVERY = "discover_cluster";

  public D2ServiceDiscoveryResponseV2 find(D2TransportClient client, String storeName) {
    return find(client, storeName, true);
  }

  public D2ServiceDiscoveryResponseV2 find(D2TransportClient client, String storeName, boolean retryOnFailure) {
    int maxAttempts = retryOnFailure ? 10 : 1;
    String requestPath = TYPE_D2_SERVICE_DISCOVERY + "/" + storeName;
    Map<String, String> requestHeaders = Collections.singletonMap(D2_SERVICE_DISCOVERY_RESPONSE_V2_ENABLED, "true");
    boolean storeNotFound = false;
    for (int attempt = 0; attempt < maxAttempts; ++attempt) {
      try {
        if (attempt > 0) {
          TimeUnit.SECONDS.sleep(3);
        }
        TransportClientResponse response = client.get(requestPath, requestHeaders).get(1, TimeUnit.SECONDS);
        if (response == null) {
          /**
           * 'null' response indicates that the Router returns 404 based on the logic in
           * {@link com.linkedin.venice.client.store.transport.TransportClientCallback}.
           * So we will treat `null` response as the store doesn't exist.
           * No need to retry the service discovery for non-existing store.
           */
          storeNotFound = true;
          break;
        }
        D2ServiceDiscoveryResponseV2 result = OBJECT_MAPPER.readValue(response.getBody(), D2ServiceDiscoveryResponseV2.class);
        if (result.isError()) {
          throw new VeniceException(result.getError());
        }
        LOGGER.info("Found d2 service {} for {}", result.getD2Service(), storeName);
        return result;

      } catch (ExecutionException e) {
        LOGGER.warn("Failed to find d2 service for {}, attempt {}/{}, reason {}", storeName, attempt + 1, maxAttempts, e.getCause());

      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ServiceDiscoveryException("Failed to find d2 service for " + storeName, e);

      } catch (Exception e) {
        throw new ServiceDiscoveryException("Failed to find d2 service for " + storeName, e);
      }
    }
    if (storeNotFound) {
      // Short circuit the retry if the store is not found.
      throw new ServiceDiscoveryException(new VeniceNoStoreException(storeName));
    }
    throw new ServiceDiscoveryException("Failed to find d2 service for " + storeName + " after " + maxAttempts + " attempts");
  }
}
