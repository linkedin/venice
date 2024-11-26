package com.linkedin.venice.stats.dimensions;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpStatusClass;


/**
 * Maps the provided HTTP response status {@link HttpResponseStatus} to one of
 * 1xx, 2xx, 3xx, 4xx, 5xx categories.
 */
public class HttpResponseStatusCodeCategory {
  private static final String UNKNOWN_CATEGORY = "unknown";

  /**
   * Private constructor to prevent instantiation of this Utility class
   */
  private HttpResponseStatusCodeCategory() {
  }

  public static String getVeniceHttpResponseStatusCodeCategory(HttpResponseStatus statusCode) {
    if (statusCode == null) {
      return UNKNOWN_CATEGORY;
    }

    HttpStatusClass statusClass = statusCode.codeClass();
    switch (statusClass) {
      case INFORMATIONAL:
        return "1xx";
      case SUCCESS:
        return "2xx";
      case REDIRECTION:
        return "3xx";
      case CLIENT_ERROR:
        return "4xx";
      case SERVER_ERROR:
        return "5xx";
      default:
        return UNKNOWN_CATEGORY;
    }
  }
}
