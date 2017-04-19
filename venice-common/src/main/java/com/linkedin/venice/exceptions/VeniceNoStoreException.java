package com.linkedin.venice.exceptions;

import org.apache.http.HttpStatus;


/**
 * Thrown when an operation should return information about a store, but the store does not exist
 */
public class VeniceNoStoreException extends VeniceException {
  private final String storeName;

  public VeniceNoStoreException(String storeName){
    super("Store: " + storeName + " does not exist");
    this.storeName = storeName;
  }

  public VeniceNoStoreException(String storeName, Throwable t){
    super("Store: " + storeName + " does not exist", t);
    this.storeName = storeName;
  }

  public String getStoreName(){
    return storeName;
  }

  @Override
  public int getHttpStatusCode() {
    return HttpStatus.SC_NOT_FOUND;
  }
}
