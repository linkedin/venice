package com.linkedin.venice.exceptions;

import org.apache.http.HttpStatus;


/**
 * Thrown when an operation should create a store, but the store already exists
 */
public class VeniceStoreAlreadyExistsException extends VeniceException {
  private final String storeName;

  public VeniceStoreAlreadyExistsException(String storeName){
    super("Store: " + storeName + " already exists");
    this.storeName = storeName;
  }

  public VeniceStoreAlreadyExistsException(String storeName, Throwable t){
    super("Store: " + storeName + " already exists", t);
    this.storeName = storeName;
  }

  public String getStoreName(){
    return storeName;
  }

  @Override
  public int getHttpStatusCode(){
    return HttpStatus.SC_CONFLICT;
  }
}
