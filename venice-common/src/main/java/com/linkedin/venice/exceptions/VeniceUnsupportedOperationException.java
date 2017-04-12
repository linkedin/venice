package com.linkedin.venice.exceptions;

import org.apache.http.HttpStatus;


public class VeniceUnsupportedOperationException extends VeniceException {
  public VeniceUnsupportedOperationException(String operation) {
    super("Operation: " + operation + " is not supported.");
  }

  public VeniceUnsupportedOperationException(String operation, Throwable t) {
    super("Operation: " + operation + " is not supported.", t);
  }

  @Override
  public int getHttpStatusCode() {
    return HttpStatus.SC_NOT_IMPLEMENTED;
  }
}
