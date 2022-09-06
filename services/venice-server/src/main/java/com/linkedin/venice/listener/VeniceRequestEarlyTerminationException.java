package com.linkedin.venice.listener;

import com.linkedin.venice.exceptions.VeniceException;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.http.HttpStatus;


public class VeniceRequestEarlyTerminationException extends VeniceException {
  public VeniceRequestEarlyTerminationException(String storeName) {
    super("The request to store: " + storeName + " is terminated because of early termination setup");
  }

  @Override
  public int getHttpStatusCode() {
    return HttpStatus.SC_REQUEST_TIMEOUT;
  }

  public static HttpResponseStatus getHttpResponseStatus() {
    return HttpResponseStatus.REQUEST_TIMEOUT;
  }
}
