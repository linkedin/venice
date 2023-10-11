package com.linkedin.venice.listener.request;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.request.RequestHelper;
import io.netty.handler.codec.http.HttpRequest;


/**
 * {@code MetadataFetchRequest} encapsulates a GET request to /metadata/storename on the storage node to fetch metadata
 * for that node.
 */
public class MetadataFetchRequest {
  private final String storeName;

  private MetadataFetchRequest(String storeName) {
    this.storeName = storeName;
  }

  public static MetadataFetchRequest parseGetHttpRequest(HttpRequest request) {
    String uri = request.uri();
    String[] requestParts = RequestHelper.getRequestParts(uri);

    if (requestParts.length == 3) {
      // [0]""/[1]"action"/[2]"store"
      String storeName = requestParts[2];
      return new MetadataFetchRequest(storeName);
    } else {
      throw new VeniceException("not a valid request for a METADATA action: " + uri);
    }
  }

  public String getStoreName() {
    return storeName;
  }
}
