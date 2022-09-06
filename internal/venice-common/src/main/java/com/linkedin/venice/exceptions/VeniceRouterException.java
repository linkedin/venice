package com.linkedin.venice.exceptions;

import io.netty.handler.codec.http.HttpResponseStatus;


public class VeniceRouterException extends VeniceException {
  public VeniceRouterException() {
    super();
  }

  public VeniceRouterException(String s) {
    super(s);
  }

  public VeniceRouterException(Throwable t) {
    super(t);
  }

  public VeniceRouterException(String s, Throwable t) {
    super(s, t);
  }

  public HttpResponseStatus getHttpResponseStatus() {
    return HttpResponseStatus.valueOf(getHttpStatusCode());
  }

}
