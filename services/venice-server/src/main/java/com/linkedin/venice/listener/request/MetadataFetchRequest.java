package com.linkedin.venice.listener.request;

import com.linkedin.venice.exceptions.VeniceException;
import io.netty.handler.codec.http.HttpRequest;


/**
 * {@code MetadataFetchRequest} encapsulates a GET request to /metadata/storeName/hash on the storage node to fetch
 * metadata for that node.
 */
public class MetadataFetchRequest {
  private final String storeName;
  private final int hash;

  private MetadataFetchRequest(String storeName, int hash) {
    this.storeName = storeName;
    this.hash = hash;
  }

  public static MetadataFetchRequest parseGetHttpRequest(HttpRequest request) {
    String uri = request.uri();
    String[] requestParts = RequestHelper.getRequestParts(uri);

    if (requestParts.length == 4) {
      // [0]""/[1]"action"/[2]"store"/[3]"hash"
      String storeName = requestParts[2];
      int hash = Integer.parseInt(requestParts[3]);
      return new MetadataFetchRequest(storeName, hash);
    } else {
      throw new VeniceException("not a valid request for a METADATA action: " + uri);
    }
  }

  public String getStoreName() {
    return storeName;
  }

  public int getHash() {
    return hash;
  }
}
