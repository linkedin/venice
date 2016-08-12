package com.linkedin.venice.exceptions;

public class StorePausedException extends VeniceException {
  public StorePausedException(String storeName, String action, int versionNumber) {
    super("Store:" + storeName + " has been pasued. Can ot accept the request to " + action + " on version:"
        + versionNumber);
  }
}
