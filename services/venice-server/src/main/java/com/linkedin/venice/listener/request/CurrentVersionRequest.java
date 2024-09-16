package com.linkedin.venice.listener.request;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.protocols.CurrentVersionInfoRequest;
import java.net.URI;


public class CurrentVersionRequest {
  private final String storeName;

  private CurrentVersionRequest(String storeName) {
    this.storeName = storeName;
  }

  public static CurrentVersionRequest parseGetHttpRequest(URI uri, String[] requestParts) {
    if (requestParts.length == 3) {
      // [0]""/[1]"action"/[2]"store"
      String storeName = requestParts[2];
      return new CurrentVersionRequest(storeName);
    } else {
      throw new VeniceException("not a valid request for a METADATA action: " + uri.getPath());
    }
  }

  public static CurrentVersionRequest parseGetGrpcRequest(CurrentVersionInfoRequest request) {
    return new CurrentVersionRequest(request.getStoreName());
  }

  public String getStoreName() {
    return storeName;
  }
}
