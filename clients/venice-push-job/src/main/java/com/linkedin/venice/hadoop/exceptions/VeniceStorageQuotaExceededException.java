package com.linkedin.venice.hadoop.exceptions;

import com.linkedin.venice.exceptions.VeniceException;


/**
 * Thrown when the input data size exceeds the store's storage quota. Raised by the Spark data writer's
 * pre-write (intermediary) quota check to fail the push before writing the data to PubSub.
 */
public class VeniceStorageQuotaExceededException extends VeniceException {
  public VeniceStorageQuotaExceededException(String message) {
    super(message);
  }

  public VeniceStorageQuotaExceededException(String message, Throwable throwable) {
    super(message, throwable);
  }
}
