package com.linkedin.venice.exceptions;

/**
 * Classes for P2P use case that are triggered by founded peers cannot connect
 */
public class VenicePeersCannotConnectException extends VeniceException {
  public VenicePeersCannotConnectException(String message) {
    super(message);
  }

  public VenicePeersCannotConnectException(String message, Throwable cause) {
    super(message, cause);
  }
}
