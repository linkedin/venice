package com.linkedin.venice.exceptions.validation;

/**
 * This exception is thrown when we detect a new producer which did not start with a {@link
 * com.linkedin.venice.kafka.protocol.enums.ControlMessageType#START_OF_SEGMENT}.
 *
 * This is a more specific case of {@link MissingDataException}, which in some cases may
 * be treated more leniently than a regular {@link MissingDataException}.
 */
public class ImproperlyStartedSegmentException extends MissingDataException {
  public ImproperlyStartedSegmentException(String message) {
    super(message);
  }

  public ImproperlyStartedSegmentException(String message, Throwable throwable) {
    super(message, throwable);
  }
}
