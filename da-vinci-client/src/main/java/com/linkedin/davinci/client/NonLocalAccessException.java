package com.linkedin.davinci.client;

import com.linkedin.venice.client.exceptions.VeniceClientException;


public class NonLocalAccessException extends VeniceClientException {
  public NonLocalAccessException() {
  }

  public NonLocalAccessException(String message) {
    super(message);
  }

  public NonLocalAccessException(Throwable exception) {
    super(exception);
  }

  public NonLocalAccessException(String message, Throwable exception) {
    super(message, exception);
  }

  public NonLocalAccessException(String version, int subPartition) {
    this("Cannot access not-subscribed partition, version=" + version + ", subPartition=" + subPartition);
  }
}
