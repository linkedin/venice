package com.linkedin.venice.exceptions;

import org.apache.http.HttpStatus;


public class VeniceUnsupportedOperationException extends VeniceException {
  public VeniceUnsupportedOperationException(String operation) {
    super(getMessage(operation));
  }

  public VeniceUnsupportedOperationException(String operation, String details) {
    super(getMessage(operation) + " " + details);
  }

  public VeniceUnsupportedOperationException(String operation, Throwable t) {
    super(getMessage(operation), t);
  }

  private static String getMessage(String operation) {
    return "Operation: " + operation + " is not supported.";
  }

  @Override
  public int getHttpStatusCode() {
    return HttpStatus.SC_NOT_IMPLEMENTED;
  }
}
