package com.linkedin.venice.exceptions.validation;

import com.linkedin.venice.utils.lazy.Lazy;


/**
 * This class encompasses all error conditions which:
 * 1. Are related to the quality of the data consumed from Kafka, and;
 * 2. Are deemed unrecoverable.
 *
 * @see MissingDataException
 * @see CorruptDataException
 */
abstract public class FatalDataValidationException extends DataValidationException {
  /**
   * N.B.: We make the message lazy because it can contain a lot of details, and assembling it is costly, so we only
   * want to pay for that if we're actually going to display it.
   */
  private final Lazy<String> message;

  public FatalDataValidationException(String message) {
    this.message = Lazy.of(() -> message);
  }

  public FatalDataValidationException(Lazy<String> message) {
    this.message = message;
  }

  @Override
  public String getMessage() {
    return this.message.get();
  }
}
