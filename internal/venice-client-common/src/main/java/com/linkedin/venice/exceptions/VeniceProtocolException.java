package com.linkedin.venice.exceptions;

public class VeniceProtocolException extends VeniceException {
  public VeniceProtocolException(String message) {
    super(message);
    super.errorType = ErrorType.PROTOCOL_ERROR;
  }
}
