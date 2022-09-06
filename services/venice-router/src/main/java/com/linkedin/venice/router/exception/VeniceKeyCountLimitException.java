package com.linkedin.venice.router.exception;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.read.RequestType;


public class VeniceKeyCountLimitException extends VeniceException {
  private final String storeName;
  private final RequestType requestType;
  private final int requestKeyCount;

  public VeniceKeyCountLimitException(
      String storeName,
      RequestType requestType,
      int requestKeyCount,
      int keyCountLimit) {
    super(
        "Request with type: " + requestType + " to store: " + storeName + " is rejected since the request key count: "
            + requestKeyCount + " exceeds key count limit: " + keyCountLimit);
    this.storeName = storeName;
    this.requestType = requestType;
    this.requestKeyCount = requestKeyCount;
  }

  public String getStoreName() {
    return storeName;
  }

  public int getRequestKeyCount() {
    return requestKeyCount;
  }

  public RequestType getRequestType() {
    return requestType;
  }
}
