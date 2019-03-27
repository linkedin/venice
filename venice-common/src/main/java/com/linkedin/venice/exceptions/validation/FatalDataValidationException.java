package com.linkedin.venice.exceptions.validation;

/**
 * This class encompasses all error conditions which:
 * 1. Are related to the quality of the data consumed from Kafka, and;
 * 2. Are deemed unrecoverable.
 *
 * @see MissingDataException
 * @see CorruptDataException
 */
abstract public class FatalDataValidationException extends DataValidationException {
  public FatalDataValidationException(String message) {
    super(message);
  }

  public FatalDataValidationException(String message, Throwable throwable) {
    super(message, throwable);
  }
}
