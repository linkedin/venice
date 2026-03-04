package com.linkedin.venice.stats.dimensions;

import io.netty.handler.codec.http.HttpResponseStatus;


/**
 * How Venice categorizes the response status of a request:
 * well as the Venice specific categorization. For instance, venice considers key not found as a healthy
 * response, but http standard would consider it a 404 (4xx) which leads to checking for both 200 and 404
 * to account for all healthy requests. This dimensions makes it easier to make Venice specific aggregations.
 */
public enum VeniceResponseStatusCategory implements VeniceDimensionInterface {
  SUCCESS, FAIL;

  /**
   * Maps an HTTP response status to the Venice-specific category.
   * In Venice, {@link HttpResponseStatus#NOT_FOUND} (key absent) is a valid/expected outcome,
   * so both {@link HttpResponseStatus#OK} and {@link HttpResponseStatus#NOT_FOUND} are classified
   * as {@link #SUCCESS}; everything else is {@link #FAIL}.
   */
  public static VeniceResponseStatusCategory getVeniceResponseStatusCategory(HttpResponseStatus responseStatus) {
    return (responseStatus.equals(HttpResponseStatus.OK) || responseStatus.equals(HttpResponseStatus.NOT_FOUND))
        ? SUCCESS
        : FAIL;
  }

  /**
   * All the instances of this Enum should have the same dimension name.
   * Refer {@link VeniceDimensionInterface#getDimensionName()} for more details.
   */
  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY;
  }
}
