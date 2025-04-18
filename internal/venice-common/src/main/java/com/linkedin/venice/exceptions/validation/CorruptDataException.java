package com.linkedin.venice.exceptions.validation;

import com.linkedin.venice.utils.lazy.Lazy;


/**
 * This exception is thrown when we detect corrupt data in the Kafka stream.
 */
public class CorruptDataException extends FatalDataValidationException {
  public CorruptDataException(Lazy<String> message) {
    super(message);
  }
}
