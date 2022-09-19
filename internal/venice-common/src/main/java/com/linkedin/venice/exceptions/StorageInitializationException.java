package com.linkedin.venice.exceptions;

/**
 * thrown by StorageEngineFactory and AbstractStorageEngine when storage creation fails
 */
public class StorageInitializationException extends VeniceException {
  private static final long serialVersionUID = 1L;

  public StorageInitializationException() {
  }

  public StorageInitializationException(String message) {
    super(message);
  }

  public StorageInitializationException(Throwable t) {
    super(t);
  }

  public StorageInitializationException(String message, Throwable t) {
    super(message, t);
  }
}
