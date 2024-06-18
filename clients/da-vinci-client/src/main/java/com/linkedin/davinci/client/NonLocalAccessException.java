package com.linkedin.davinci.client;

import com.linkedin.venice.client.exceptions.VeniceClientException;


public class NonLocalAccessException extends VeniceClientException {
  public NonLocalAccessException(String message) {
    super(message);
  }

  public NonLocalAccessException(String version, int partition) {
    this("Cannot access not-subscribed partition, version=" + version + ", partition=" + partition);
  }
}
