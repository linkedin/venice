package com.linkedin.venice.client.exceptions;

public class VeniceClientRateExceededException extends VeniceClientHttpException {

  // This isn't defined in org.apache.http.HttpStatus
  // defining here instead of depending on Netty's HttpResponseStatus
  public static final int HTTP_TOO_MANY_REQUESTS = 429;

  public VeniceClientRateExceededException(String msg) {
    super(msg, HTTP_TOO_MANY_REQUESTS);
  }

  public VeniceClientRateExceededException() {
    super(HTTP_TOO_MANY_REQUESTS);
  }
}
