package com.linkedin.venice.listener.response;

import com.linkedin.venice.response.VeniceReadResponseStatus;
import io.netty.handler.codec.http.HttpResponseStatus;


/** A response object carrying an HTTP status and optional Venice read response status. */
public class HttpShortcutResponse {
  private final String message;
  private final HttpResponseStatus status;
  private final VeniceReadResponseStatus veniceReadResponseStatus;

  private boolean misroutedStoreVersion = false;

  public HttpShortcutResponse(String message, HttpResponseStatus status) {
    this(message, status, null);
  }

  public HttpShortcutResponse(HttpResponseStatus status) {
    this("", status, null);
  }

  public HttpShortcutResponse(String message, HttpResponseStatus status, VeniceReadResponseStatus readResponseStatus) {
    this.message = message;
    this.status = status;
    this.veniceReadResponseStatus = readResponseStatus;
  }

  public String getMessage() {
    return message;
  }

  public HttpResponseStatus getStatus() {
    return status;
  }

  public VeniceReadResponseStatus getVeniceReadResponseStatus() {
    return veniceReadResponseStatus;
  }

  public boolean isMisroutedStoreVersion() {
    return misroutedStoreVersion;
  }

  public void setMisroutedStoreVersion(boolean misroutedStoreVersion) {
    this.misroutedStoreVersion = misroutedStoreVersion;
  }
}
