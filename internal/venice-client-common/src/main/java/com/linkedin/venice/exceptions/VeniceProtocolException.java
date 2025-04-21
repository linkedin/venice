package com.linkedin.venice.exceptions;

public class VeniceProtocolException extends VeniceException {
  public VeniceProtocolException(String message) {
    super(message);
    super.errorType = ErrorType.PROTOCOL_ERROR;
  }

  public VeniceProtocolException(String message, Throwable throwable) {
    super(message, throwable, ErrorType.PROTOCOL_ERROR);
  }
}
