package com.linkedin.venice.exceptions;

import org.apache.http.HttpStatus;


public class ResourceStillExistsException extends VeniceException {
  public ResourceStillExistsException(String message) {
    super(message);
    super.errorType = ErrorType.RESOURCE_STILL_EXISTS;
  }

  @Override
  public int getHttpStatusCode() {
    return HttpStatus.SC_PRECONDITION_FAILED;
  }
}
