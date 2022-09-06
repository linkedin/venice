package com.linkedin.venice.exceptions;

public class StoreDisabledException extends VeniceException {
  public StoreDisabledException(String storeName, String action, int versionNumber) {
    super(
        "Store:" + storeName + " has been disabled. Can not accept the request to " + action + " on version:"
            + versionNumber);
  }

  public StoreDisabledException(String storeName, String action) {
    super("Store:" + storeName + " has been disabled. Can not accept the request to " + action);
  }
}
