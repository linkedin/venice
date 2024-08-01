package com.linkedin.venice.exceptions;

/**
 * Classes for P2P usecase that are triggered by peers not found
 */
public class VenicePeersNotFoundException extends VeniceException {
  public VenicePeersNotFoundException(String message) {
    super(message);
  }

  public VenicePeersNotFoundException(String message, Throwable cause) {
    super(message, cause);
  }
}
