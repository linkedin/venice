package com.linkedin.venice.exceptions.validation;

/**
 * This exception is thrown when we detect missing data in the Kafka stream.
 */
public class MissingDataException extends FatalDataValidationException {
  public MissingDataException(String message) {
    super(message);
  }

  public MissingDataException(String message, Throwable throwable) {
    super(message, throwable);
  }
}
