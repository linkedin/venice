package com.linkedin.venice.exceptions;

/**
 * Bootstrap exception when starting
 */
public class VeniceBootstrapException extends VeniceException {
  public VeniceBootstrapException(String message) {
    super(message);
  }

  public VeniceBootstrapException(String message, Throwable cause) {
    super(message, cause);
  }
}
