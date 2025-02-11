package com.linkedin.venice.exceptions;

/**
 * Classes for P2P use case that are triggered by blob transfer file not found in target host
 */
public class VeniceBlobTransferFileNotFoundException extends VeniceException {
  public VeniceBlobTransferFileNotFoundException(String message) {
    super(message);
  }

  public VeniceBlobTransferFileNotFoundException(String message, Throwable cause) {
    super(message, cause);
  }
}
