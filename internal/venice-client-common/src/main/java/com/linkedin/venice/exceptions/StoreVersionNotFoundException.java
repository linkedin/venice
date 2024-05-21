package com.linkedin.venice.exceptions;

public class StoreVersionNotFoundException extends VeniceException {
  public StoreVersionNotFoundException(String storeName, int versionNumber) {
    super("Could not find store-version! Store: " + storeName + "; version: " + versionNumber + ".");
  }

  public StoreVersionNotFoundException(String topicName) {
    super("Could not find store-version: " + topicName);
  }
}
