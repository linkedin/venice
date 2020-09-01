package com.linkedin.venice.serializer;

import com.linkedin.venice.exceptions.VeniceException;


public class VeniceSerializationException extends VeniceException {
  public VeniceSerializationException(String message, Throwable throwable) {
    super(message, throwable);
  }

  public VeniceSerializationException(String message) {
    super(message);
  }
}
