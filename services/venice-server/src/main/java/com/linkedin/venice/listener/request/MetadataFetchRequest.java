package com.linkedin.venice.listener.request;

import com.linkedin.venice.exceptions.VeniceException;
import java.util.Optional;


/**
 * {@code MetadataFetchRequest} encapsulates a GET request to /metadata/storename on the storage node to fetch metadata
 * for that node.
 */
public class MetadataFetchRequest {
  private final String storeName;
  private final Optional<String> clientName;

  private MetadataFetchRequest(String storeName, Optional<String> clientName) {
    this.storeName = storeName;
    this.clientName = clientName;
  }

  public static MetadataFetchRequest parseGetHttpRequest(String uri, String[] requestParts) {
    String storeName;
    Optional<String> clientName = Optional.empty();

    // requestParts
    // - 0: empty
    // - 1: action [string]
    // - 2: storeName [string]
    // - 3: clientName [string]

    // Waterfall switch
    switch (requestParts.length) {
      case 4:
        clientName = Optional.ofNullable(requestParts[3]);
      case 3:
        storeName = requestParts[2];
        break;
      default:
        throw new VeniceException("not a valid request for a METADATA action: " + uri);
    }

    return new MetadataFetchRequest(storeName, clientName);
  }

  public String getStoreName() {
    return this.storeName;
  }

  public Optional<String> getClientName() {
    return this.clientName;
  }
}
