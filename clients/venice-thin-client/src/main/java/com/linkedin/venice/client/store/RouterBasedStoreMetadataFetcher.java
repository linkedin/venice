package com.linkedin.venice.client.store;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.client.store.transport.D2TransportClient;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.controllerapi.MultiStoreResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.ObjectMapperFactory;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;


/**
 * Router-based implementation for fetching store metadata that is not cluster-specific.
 * Unlike {@link com.linkedin.venice.client.schema.RouterBasedStoreSchemaFetcher}, this class
 * is not tied to a specific store and operates on metadata available globally across clusters.
 */
public class RouterBasedStoreMetadataFetcher implements StoreMetadataFetcher {
  public static final String TYPE_STORES = "stores";

  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();

  // Ignore unknown fields while parsing json response.
  static {
    OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  }

  private final TransportClient transportClient;

  public RouterBasedStoreMetadataFetcher(D2Client d2Client, String d2ServiceName) {
    this.transportClient = new D2TransportClient(d2ServiceName, d2Client);
  }

  // VisibleForTesting
  RouterBasedStoreMetadataFetcher(TransportClient transportClient) {
    this.transportClient = transportClient;
  }

  /**
   * Returns all store names available across all clusters, as seen by the router's
   * non-cluster-specific {@link com.linkedin.venice.helix.HelixReadOnlyStoreConfigRepository}.
   */
  @Override
  public Set<String> getAllStoreNames() {
    byte[] responseBody;
    try {
      TransportClientResponse response = transportClient.get(TYPE_STORES, Collections.emptyMap()).get();
      if (response == null) {
        throw new VeniceException("Received null response from router for path: " + TYPE_STORES);
      }
      responseBody = response.getBody();
      if (responseBody == null) {
        throw new VeniceException("Received empty response body from router for path: " + TYPE_STORES);
      }
    } catch (ExecutionException | InterruptedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw new VeniceException("Failed to fetch store names from router", e);
    }

    MultiStoreResponse multiStoreResponse;
    try {
      multiStoreResponse = OBJECT_MAPPER.readValue(responseBody, MultiStoreResponse.class);
    } catch (IOException e) {
      throw new VeniceException("Failed to deserialize store names response", e);
    }

    if (multiStoreResponse.isError()) {
      throw new VeniceException("Received error while fetching store names: " + multiStoreResponse.getError());
    }

    String[] stores = multiStoreResponse.getStores();
    if (stores == null) {
      return Collections.emptySet();
    }
    return new HashSet<>(Arrays.asList(stores));
  }

  @Override
  public void close() throws IOException {
    transportClient.close();
  }
}
