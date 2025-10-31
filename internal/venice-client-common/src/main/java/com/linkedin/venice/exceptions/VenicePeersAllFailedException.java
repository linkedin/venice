package com.linkedin.venice.exceptions;

/**
 * Exception thrown when all discovered peers are unable to fulfill a successful blob transfer.
 */
public class VenicePeersAllFailedException extends VeniceException {
  public VenicePeersAllFailedException(String message) {
    super(message);
  }

  public VenicePeersAllFailedException(String message, Throwable cause) {
    super(message, cause);
  }
}
