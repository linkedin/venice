package com.linkedin.venice.stats.dimensions;

/**
 * How Venice categorizes the response status of a request:
 * We are emitting both {@link HttpResponseStatusCodeCategory} and this enum to capture the http standard as
 * well as the Venice specific categorization. For instance, venice considers key not found as a healthy
 * response, but http standard would consider it a 404 (4xx) which leads to checking for both 200 and 404
 * to account for all healthy requests. This dimensions makes it easier to make Venice specific aggregations.
 */
public enum VeniceResponseStatusCategory {
  SUCCESS, FAIL;

  private final String category;

  VeniceResponseStatusCategory() {
    this.category = name().toLowerCase();
  }

  public String getCategory() {
    return this.category;
  }
}
