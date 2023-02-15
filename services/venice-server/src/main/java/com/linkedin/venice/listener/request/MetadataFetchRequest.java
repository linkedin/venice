package com.linkedin.venice.listener.request;

import com.linkedin.venice.exceptions.VeniceException;
import io.netty.handler.codec.http.HttpRequest;
import java.net.URI;


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
    /**
     * Sometimes req.uri() gives a full uri (e.g. https://host:port/path) and sometimes it only gives a path.
     * Generating a URI lets us always take just the path, but we need to add on the query string.
     */
    URI fullUri = URI.create(uri);
    String path = fullUri.getRawPath();
    if (fullUri.getRawQuery() != null) {
      path += "?" + fullUri.getRawQuery();
    }
    String[] requestParts = path.split("/");

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
