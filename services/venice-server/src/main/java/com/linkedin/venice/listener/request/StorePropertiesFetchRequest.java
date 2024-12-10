package com.linkedin.venice.listener.request;

import com.linkedin.venice.exceptions.VeniceException;


/**
 * {@code MetadataFetchRequest} encapsulates a GET request to /store_properties/storename on the storage node to fetch metadata
 * for that node.
 */
public class StorePropertiesFetchRequest {
  private final String storeName;

  private StorePropertiesFetchRequest(String storeName) {
    this.storeName = storeName;
  }

  public static StorePropertiesFetchRequest parseGetHttpRequest(String uri, String[] requestParts) {
    if (requestParts.length != 3) {
      throw new VeniceException("not a valid request for a Store Properties action: " + uri);
    }

    String storeName = requestParts[2];
    return new StorePropertiesFetchRequest(storeName);
  }

  public String getStoreName() {
    return this.storeName;
  }
}
