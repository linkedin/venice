package com.linkedin.venice.exceptions;

/**
 * This exception is thrown when we detect inconsistent or possibly corrupted store metadata on storage nodes.
 */
public class VeniceInconsistentStoreMetadataException extends VeniceException {
  public VeniceInconsistentStoreMetadataException(String message) {
    super(message);
  }
}
