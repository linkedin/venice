package com.linkedin.venice.stats.dimensions;

import io.netty.handler.codec.http.HttpResponseStatus;


/**
 * Maps the provided HTTP response status {@link HttpResponseStatus} to one of
 * 1xx, 2xx, 3xx, 4xx, 5xx categories.
 */
public enum HttpResponseStatusCodeCategory implements VeniceDimensionInterface {
  INFORMATIONAL("1xx"), SUCCESS("2xx"), REDIRECTION("3xx"), CLIENT_ERROR("4xx"), SERVER_ERROR("5xx"),
  UNKNOWN("unknown");

  private final String category;

  HttpResponseStatusCodeCategory(String category) {
    this.category = category;
  }

  public static HttpResponseStatusCodeCategory getVeniceHttpResponseStatusCodeCategory(HttpResponseStatus statusCode) {
    if (statusCode == null) {
      return UNKNOWN;
    }

    try {
      return HttpResponseStatusCodeCategory.valueOf(statusCode.codeClass().name());
    } catch (IllegalArgumentException e) {
      return UNKNOWN;
    }
  }

  /**
   * All the instances of this Enum should have the same dimension name.
   * Refer {@link VeniceDimensionInterface#getDimensionName()} for more details.
   */
  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE_CATEGORY;
  }

  @Override
  public String getDimensionValue() {
    return category;
  }
}
