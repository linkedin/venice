package com.linkedin.venice.exceptions.validation;

/**
 * This is a benign {@link DataValidationException}.
 *
 * It indicates that duplicate data was consumed from the Kafka stream.
 *
 * The receiver of this exception is free to carry on, but may choose to
 * forgo any processing it had planned to do about the duplicate data.
 */
public class DuplicateDataException extends DataValidationException {
  public DuplicateDataException(String message) {
    super(message);
  }

  public DuplicateDataException(String message, Throwable throwable) {
    super(message, throwable);
  }
}
