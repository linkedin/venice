package com.linkedin.venice.storage;

/**
 * Custom Exception for Venice Storage Related Issues.
 * Currently everything is inherited from the superclass.
 */
public class VeniceStorageException extends Exception {

  /** */
  private static final long serialVersionUID = 1L;

  public VeniceStorageException(String message) {
    super(message);
  }

  public VeniceStorageException(String message, Throwable throwable) {
    super(message, throwable);
  }

  public String getMessage() {
    return super.getMessage();
  }
}
