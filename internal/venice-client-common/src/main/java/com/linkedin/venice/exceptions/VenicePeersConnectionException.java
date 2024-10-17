package com.linkedin.venice.exceptions;

/**
 * Classes for P2P use case that are triggered by founded peers cannot connect
 */
public class VenicePeersConnectionException extends VeniceException {
  public VenicePeersConnectionException(String message) {
    super(message);
  }

  public VenicePeersConnectionException(String message, Throwable cause) {
    super(message, cause);
  }
}
