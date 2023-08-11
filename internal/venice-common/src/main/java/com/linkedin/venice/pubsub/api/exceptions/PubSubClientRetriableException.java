package com.linkedin.venice.pubsub.api.exceptions;

import com.linkedin.venice.exceptions.VeniceRetriableException;


public class PubSubClientRetriableException extends VeniceRetriableException {
  public PubSubClientRetriableException(String message) {
    super(message);
  }

  public PubSubClientRetriableException(String message, Throwable cause) {
    super(message, cause);
  }
}
