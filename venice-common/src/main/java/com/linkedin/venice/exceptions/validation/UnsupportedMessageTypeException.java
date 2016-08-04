package com.linkedin.venice.exceptions.validation;

/**
 * This is thrown when some code encounters a message type it does not know about.
 *
 * It may indicate that there is corrupt data which happened to touch the message
 * type section of the data. Or it may indicate that some service was upgraded to
 * a more recent version without properly planning the rollout process.
 */
public class UnsupportedMessageTypeException extends CorruptDataException {
  public UnsupportedMessageTypeException(String exceptionMessage) {
    super(exceptionMessage);
  }
}
