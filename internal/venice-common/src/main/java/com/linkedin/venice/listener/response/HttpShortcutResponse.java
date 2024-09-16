package com.linkedin.venice.listener.response;

import io.netty.handler.codec.http.HttpResponseStatus;


/**
 * Created by mwise on 3/11/16.
 */
public class HttpShortcutResponse {
  private final String message;
  private final HttpResponseStatus status;

  public HttpShortcutResponse(String message, HttpResponseStatus status) {
    this.message = message;
    this.status = status;
  }

  public HttpShortcutResponse(HttpResponseStatus status) {
    this("", status);
  }

  public String getMessage() {
    return message;
  }

  public HttpResponseStatus getStatus() {
    return status;
  }
}
