package com.linkedin.venice.exceptions;

public class InvalidVeniceSchemaException extends VeniceException {
  protected ExceptionType exceptionType = ExceptionType.INVALID_SCHEMA;

  public InvalidVeniceSchemaException(String message) { super(message); }

  public InvalidVeniceSchemaException(String message, Throwable cause) { super(message, cause);}
}
