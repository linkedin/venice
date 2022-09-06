package com.linkedin.venice.exceptions.validation;

/**
 * This exception is thrown when we detect corrupt data in the Kafka stream.
 */
public class CorruptDataException extends FatalDataValidationException {
  public CorruptDataException(String message) {
    super(message);
  }

  public CorruptDataException(String message, Throwable throwable) {
    super(message, throwable);
  }
}
