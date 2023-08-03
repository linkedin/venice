package com.linkedin.venice.pubsub.api.exceptions;

import com.linkedin.venice.exceptions.VeniceException;


public class PubSubClientException extends VeniceException {
  public PubSubClientException(String message) {
    super(message);
  }

  public PubSubClientException(String message, Throwable cause) {
    super(message, cause);
  }
}
