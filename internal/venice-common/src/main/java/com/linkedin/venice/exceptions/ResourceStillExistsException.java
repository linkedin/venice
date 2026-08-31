package com.linkedin.venice.exceptions;

public class ResourceStillExistsException extends VeniceException {
  public ResourceStillExistsException(String message) {
    super(message);
    super.errorType = ErrorType.RESOURCE_STILL_EXISTS;
  }
}
