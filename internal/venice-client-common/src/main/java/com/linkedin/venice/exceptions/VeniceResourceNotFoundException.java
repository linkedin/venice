package com.linkedin.venice.exceptions;

/**
 * Class for all Venice exceptions that are triggered by resources not being found.
 */
public class VeniceResourceNotFoundException extends VeniceException {
  public VeniceResourceNotFoundException(String message) {
    super(message);
  }

  public VeniceResourceNotFoundException(String message, Throwable cause) {
    super(message, cause);
  }
}
