package com.linkedin.venice.exceptions;

/**
 * Used when there is a timeout from waiting for an admin message to be consumed
 */
public class AdminMessageConsumptionTimeoutException extends VeniceException {
  public AdminMessageConsumptionTimeoutException(String message, Throwable t) {
    super(message, t);
  }
}
