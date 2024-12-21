package com.linkedin.venice.listener.request;

import com.linkedin.venice.exceptions.VeniceException;
import java.util.Optional;


/**
 * {@code MetadataFetchRequest} encapsulates a GET request to /store_properties/storename on the storage node to fetch metadata
 * for that node.
 */
public class StorePropertiesFetchRequest {
  private final String storeName;
  private final Optional<Integer> largestKnownSchemaId;

  private StorePropertiesFetchRequest(String storeName, Optional<Integer> largestKnownSchemaId) {
    this.storeName = storeName;
    this.largestKnownSchemaId = largestKnownSchemaId;
  }

  public static StorePropertiesFetchRequest parseGetHttpRequest(String uri, String[] requestParts) {

    String storeName;
    Optional<Integer> largestKnownSchemaId;

    switch (requestParts.length) {
      case 3:
        storeName = requestParts[2];
        largestKnownSchemaId = Optional.empty();
        break;
      case 4:
        storeName = requestParts[2];
        largestKnownSchemaId = Optional.of(Integer.parseInt(requestParts[3]));
        break;
      default:
        throw new VeniceException("not a valid request for a Store Properties action: " + uri);
    }

    return new StorePropertiesFetchRequest(storeName, largestKnownSchemaId);
  }

  public String getStoreName() {
    return this.storeName;
  }

  public Optional<Integer> getLargestKnownSchemaId() {
    return this.largestKnownSchemaId;
  }
}
