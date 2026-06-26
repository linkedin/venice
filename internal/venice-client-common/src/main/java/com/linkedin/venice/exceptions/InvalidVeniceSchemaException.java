package com.linkedin.venice.exceptions;

import org.apache.http.HttpStatus;


public class InvalidVeniceSchemaException extends VeniceException {
  private static final String UNABLE_TO_FIND_VALUE_SCHEMA = "Unable to find value schema id: %s for store: %s";

  public InvalidVeniceSchemaException(String message) {
    super(message);
    super.errorType = ErrorType.INVALID_SCHEMA;
  }

  public InvalidVeniceSchemaException(String message, Throwable cause) {
    super(message, cause);
    super.errorType = ErrorType.INVALID_SCHEMA;
  }

  public InvalidVeniceSchemaException(String storeName, String schemaId) {
    super(String.format(UNABLE_TO_FIND_VALUE_SCHEMA, storeName, schemaId));
    super.errorType = ErrorType.INVALID_SCHEMA;
  }

  @Override
  public int getHttpStatusCode() {
    return HttpStatus.SC_BAD_REQUEST;
  }
}
