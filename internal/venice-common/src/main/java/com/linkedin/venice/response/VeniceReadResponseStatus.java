package com.linkedin.venice.response;

/**
 * Response status codes for Venice read requests.
 */
public class VeniceReadResponseStatus {
  public static final int DEADLINE_EXCEEDED = -504;
  public static final int KEY_NOT_FOUND = -420;

  public static final int OK = 200;
  public static final int BAD_REQUEST = 400;
  public static final int INTERNAL_ERROR = 500;
  public static final int TOO_MANY_REQUESTS = 429;
  public static final int SERVICE_UNAVAILABLE = 503;
}
