package com.linkedin.venice.exceptions;

public class VeniceChecksumException extends VeniceException {
  public VeniceChecksumException(String msg) {
    super(msg);
  }

  public VeniceChecksumException(String msg, Throwable t) {
    super(msg, t);
  }
}
