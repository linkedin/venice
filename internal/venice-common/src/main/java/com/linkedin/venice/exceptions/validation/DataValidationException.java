package com.linkedin.venice.exceptions.validation;

import com.linkedin.venice.exceptions.KafkaConsumerException;


/**
 * This class encompasses all error conditions which are related to the quality of
 * the data consumed from Kafka. Some of these exceptions represent unrecoverable
 * conditions, while others may be benign.
 *
 * @see FatalDataValidationException, which includes:
 * @see   CorruptDataException
 * @see   MissingDataException
 * // Benign data validation exception, which includes::
 * @see   DuplicateDataException
 */
public abstract class DataValidationException extends KafkaConsumerException {
  public DataValidationException(String message) {
    super(message);
  }

  public DataValidationException(String message, Throwable throwable) {
    super(message, throwable);
  }
}
