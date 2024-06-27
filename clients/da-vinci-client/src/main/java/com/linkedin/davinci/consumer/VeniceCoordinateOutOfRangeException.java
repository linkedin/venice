package com.linkedin.davinci.consumer;

import com.linkedin.venice.exceptions.VeniceException;


public class VeniceCoordinateOutOfRangeException extends VeniceException {
  public VeniceCoordinateOutOfRangeException(String message) {
    super(message);
  }

  public VeniceCoordinateOutOfRangeException(String message, Throwable cause) {
    super(message, cause);
  }

  public VeniceCoordinateOutOfRangeException(Throwable cause) {
    super(cause);
  }
}
