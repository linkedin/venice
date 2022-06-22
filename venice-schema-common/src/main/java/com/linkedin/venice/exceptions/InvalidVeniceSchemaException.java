package com.linkedin.venice.exceptions;

public class InvalidVeniceSchemaException extends VeniceException {
  public InvalidVeniceSchemaException(String message) {
    super(message);
    super.exceptionType = ExceptionType.INVALID_SCHEMA;
  }

  public InvalidVeniceSchemaException(String message, Throwable cause) {
    super(message, cause);
    super.exceptionType = ExceptionType.INVALID_SCHEMA;
  }
}
