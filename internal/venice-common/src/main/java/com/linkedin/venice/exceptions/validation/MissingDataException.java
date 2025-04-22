package com.linkedin.venice.exceptions.validation;

import com.linkedin.venice.utils.lazy.Lazy;


/**
 * This exception is thrown when we detect missing data in the Kafka stream.
 */
public class MissingDataException extends FatalDataValidationException {
  public MissingDataException(String message) {
    super(message);
  }

  public MissingDataException(Lazy<String> message) {
    super(message);
  }
}
