package com.linkedin.venice.stats.dimensions;

/**
 * How Venice categorizes the response status of a request:
 * well as the Venice specific categorization. For instance, venice considers key not found as a healthy
 * response, but http standard would consider it a 404 (4xx) which leads to checking for both 200 and 404
 * to account for all healthy requests. This dimensions makes it easier to make Venice specific aggregations.
 */
public enum VeniceResponseStatusCategory implements VeniceDimensionInterface {
  SUCCESS, FAIL;

  private final String category;;

  VeniceResponseStatusCategory() {
    this.category = name().toLowerCase();
  }

  /**
   * All the instances of this Enum should have the same dimension name.
   * Refer {@link VeniceDimensionInterface#getDimensionName()} for more details.
   */
  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY;
  }

  @Override
  public String getDimensionValue() {
    return this.category;
  }
}
