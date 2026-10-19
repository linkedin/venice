package com.linkedin.venice.hadoop;

import com.linkedin.venice.exceptions.VeniceException;


public class DictionaryBuildInterruptedException extends VeniceException {
  public DictionaryBuildInterruptedException(long elapsedMinutes, Throwable cause) {
    super("Compression dictionary build interrupted after " + elapsedMinutes + " min", cause);
  }
}
