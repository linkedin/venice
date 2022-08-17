package com.linkedin.venice.exceptions;

/**
 * Thrown by the StorageEngine or Storage Partitions if storage fails
 *
 */
public class PersistenceFailureException extends VeniceException {
  private static final long serialVersionUID = 1L;

  public PersistenceFailureException() {
    super();
  }

  public PersistenceFailureException(String s) {
    super(s);
  }

  public PersistenceFailureException(Throwable t) {
    super(t);
  }

  public PersistenceFailureException(String s, Throwable t) {
    super(s, t);
  }
}
