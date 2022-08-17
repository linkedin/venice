package com.linkedin.venice.serializer;

import com.linkedin.venice.exceptions.VeniceException;


public class VsonSerializationException extends VeniceException {
  protected VsonSerializationException() {
    super();
  }

  public VsonSerializationException(String s, Throwable t) {
    super(s, t);
  }

  public VsonSerializationException(String s) {
    super(s);
  }

  public VsonSerializationException(Throwable t) {
    super(t);
  }

}
