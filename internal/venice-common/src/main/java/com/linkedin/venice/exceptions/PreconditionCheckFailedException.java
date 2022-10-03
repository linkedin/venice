package com.linkedin.venice.exceptions;

import org.apache.http.HttpStatus;


public class PreconditionCheckFailedException extends VeniceException {
  public PreconditionCheckFailedException(String message) {
    super(message);
    super.errorType = ErrorType.PRECONDITION_CHECK_FAILED;
  }

  @Override
  public int getHttpStatusCode() {
    return HttpStatus.SC_PRECONDITION_FAILED;
  }
}
