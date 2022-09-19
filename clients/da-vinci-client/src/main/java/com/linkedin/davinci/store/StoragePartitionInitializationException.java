package com.linkedin.davinci.store;

import com.linkedin.venice.exceptions.VeniceException;


/**
 * This exception indicates that the server was unable to initialize on or more
 * storage services or stores within a service.
 */
public class StoragePartitionInitializationException extends VeniceException {
  private static final long serialVersionUID = 1;

  public StoragePartitionInitializationException() {
  }

  public StoragePartitionInitializationException(String message) {
    super(message);
  }

  public StoragePartitionInitializationException(Throwable t) {
    super(t);
  }

  public StoragePartitionInitializationException(String message, Throwable t) {
    super(message, t);
  }

}
