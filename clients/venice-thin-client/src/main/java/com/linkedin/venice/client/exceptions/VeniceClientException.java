package com.linkedin.venice.client.exceptions;

import com.linkedin.venice.exceptions.VeniceException;


/***
 * Thrown by the venice thin client if something goes wrong with the request
 * Parent class to VeniceClientHttpException
 */
public class VeniceClientException extends VeniceException {
  public VeniceClientException(String msg, Throwable e) {
    super(msg, e);
  }

  public VeniceClientException(Throwable e) {
    super(e);
  }

  public VeniceClientException(String msg) {
    super(msg);
  }

  public VeniceClientException() {
    super();
  }
}
