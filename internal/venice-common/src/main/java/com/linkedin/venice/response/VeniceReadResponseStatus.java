package com.linkedin.venice.response;

/**
 * Enumeration of response status codes for Venice read requests.
 * <p>
 * **Positive values** correspond to standard HTTP status codes and can be used directly in HTTP responses.
 * **Negative values** represent custom Venice-specific error codes.
 * <p>
 * For example, a status code of `200` indicates a successful read, while a status code of `-100` might indicate a specific Venice-related error.
 */
public class VeniceReadResponseStatus {
  public static final int KEY_NOT_FOUND = -420;

  public static final int OK = 200;
  public static final int BAD_REQUEST = 400;
  public static final int INTERNAL_ERROR = 500;
  public static final int TOO_MANY_REQUESTS = 429;
  public static final int SERVICE_UNAVAILABLE = 503;
}
