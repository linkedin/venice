package com.linkedin.venice.exceptions;

/**
 * Throw this exception when adding new schema, which already exists.
 */
public class StoreValueSchemaExistException extends VeniceException {
  public StoreValueSchemaExistException(String storeName, String schemaStr) {
    super("Value schema: [" + schemaStr + "] of Store: " + storeName + " already exists");
  }
}
