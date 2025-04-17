package com.linkedin.venice.hadoop.exceptions;

import com.linkedin.venice.exceptions.VeniceException;


public class VeniceInvalidInputException extends VeniceException {
  public VeniceInvalidInputException(String message) {
    super(message);
  }

  public VeniceInvalidInputException(String message, Throwable throwable) {
    super(message, throwable);
  }
}
