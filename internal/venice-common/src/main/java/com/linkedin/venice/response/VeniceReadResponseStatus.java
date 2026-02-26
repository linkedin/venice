package com.linkedin.venice.response;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


/**
 * Enumeration of response status codes for Venice read requests.
 * <p>
 * Positive values correspond to standard HTTP status codes and can be used directly in HTTP responses.
 * Negative values represent custom Venice-specific error codes.
 * <p>
 * {@link #UNKNOWN} is a sentinel value (code {@code -1}) returned by {@link #fromCode(int)} when no matching status
 * is found. The code {@code -1} is reserved and must never be used as a real status code.
 */
public enum VeniceReadResponseStatus {
  UNKNOWN(-1), KEY_NOT_FOUND(-420), OK(200), BAD_REQUEST(400), TOO_MANY_REQUESTS(429), INTERNAL_ERROR(500),
  SERVICE_UNAVAILABLE(503);

  private final int code;

  private static final Map<Integer, VeniceReadResponseStatus> CODE_MAP;

  static {
    Map<Integer, VeniceReadResponseStatus> map = new HashMap<>();
    for (VeniceReadResponseStatus status: values()) {
      if (status != UNKNOWN) {
        map.put(status.code, status);
      }
    }
    CODE_MAP = Collections.unmodifiableMap(map);
  }

  VeniceReadResponseStatus(int code) {
    this.code = code;
  }

  public int getCode() {
    return code;
  }

  /**
   * Returns the {@link VeniceReadResponseStatus} for the given integer code, or {@link #UNKNOWN} if no match is found.
   */
  public static VeniceReadResponseStatus fromCode(int code) {
    return CODE_MAP.getOrDefault(code, UNKNOWN);
  }
}
