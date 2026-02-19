package com.linkedin.venice.response;

import java.util.HashMap;
import java.util.Map;


/**
 * Enumeration of response status codes for Venice read requests.
 * <p>
 * Positive values correspond to standard HTTP status codes and can be used directly in HTTP responses.
 * Negative values represent custom Venice-specific error codes.
 */
public enum VeniceReadResponseStatus {
  KEY_NOT_FOUND(-420), OK(200), BAD_REQUEST(400), METHOD_NOT_ALLOWED(405), REQUEST_TIMEOUT(408),
  MISROUTED_STORE_VERSION(410), TOO_MANY_REQUESTS(429), INTERNAL_ERROR(500), SERVICE_UNAVAILABLE(503);

  private final int code;

  private static final Map<Integer, VeniceReadResponseStatus> CODE_MAP = new HashMap<>();

  static {
    for (VeniceReadResponseStatus status: values()) {
      CODE_MAP.put(status.code, status);
    }
  }

  VeniceReadResponseStatus(int code) {
    this.code = code;
  }

  public int getCode() {
    return code;
  }

  /**
   * Returns the {@link VeniceReadResponseStatus} for the given integer code, or {@code null} if no match is found.
   */
  public static VeniceReadResponseStatus fromCode(int code) {
    return CODE_MAP.get(code);
  }
}
