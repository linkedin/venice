package com.linkedin.venice.exceptions;

/**
 * Throw this exception when try to add key schema to a store, whose key schema already exists.
 */
public class StoreKeySchemaExistException extends VeniceException {
  private StoreKeySchemaExistException(String storeName) {
    super("Key schema of Store: " + storeName + " already exists");
  }

  public static StoreKeySchemaExistException newExceptionForStore(String storeName) {
    return new StoreKeySchemaExistException(storeName);
  }
}
