package com.linkedin.venice.response;

import io.netty.handler.codec.http.HttpResponseStatus;
import java.util.HashMap;
import java.util.Map;


/**
 * Defines response status codes for Venice read requests. This wrapper around {@link HttpResponseStatus} allows
 * for the inclusion of custom status codes that extend beyond the standard HTTP status codes.
 */
public enum VeniceReadResponseStatus {
  KEY_NOT_FOUND(HttpResponseStatus.NOT_FOUND), OK(HttpResponseStatus.OK), BAD_REQUEST(HttpResponseStatus.BAD_REQUEST),
  FORBIDDEN(HttpResponseStatus.FORBIDDEN), METHOD_NOT_ALLOWED(HttpResponseStatus.METHOD_NOT_ALLOWED),
  REQUEST_TIMEOUT(HttpResponseStatus.REQUEST_TIMEOUT), TOO_MANY_REQUESTS(HttpResponseStatus.TOO_MANY_REQUESTS),
  INTERNAL_SERVER_ERROR(HttpResponseStatus.INTERNAL_SERVER_ERROR),
  SERVICE_UNAVAILABLE(HttpResponseStatus.SERVICE_UNAVAILABLE), PARTIAL_RESPONSE(HttpResponseStatus.PARTIAL_CONTENT),
  MISROUTED_STORE_VERSION(new HttpResponseStatus(570, "Misrouted request"));

  private static final Map<Integer, VeniceReadResponseStatus> STATUS_MAP = new HashMap<>(16);

  static {
    for (VeniceReadResponseStatus status: values()) {
      STATUS_MAP.put(status.getCode(), status);
    }
  }

  private final HttpResponseStatus httpResponseStatus;

  VeniceReadResponseStatus(HttpResponseStatus httpResponseStatus) {
    this.httpResponseStatus = httpResponseStatus;
  }

  public HttpResponseStatus getHttpResponseStatus() {
    return httpResponseStatus;
  }

  public int getCode() {
    return httpResponseStatus.code();
  }

  public static VeniceReadResponseStatus fromCode(int code) {
    VeniceReadResponseStatus status = STATUS_MAP.get(code);
    if (status == null) {
      throw new IllegalArgumentException("Unknown status venice read response status code: " + code);
    }
    return status;
  }
}
