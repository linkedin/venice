package com.linkedin.venice.stats.dimensions;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_GRANULARITY;


public enum Granularity implements VeniceDimensionInterface {
  SERIALIZATION, DESERIALIZATION, DECOMPRESSION, SUBMISSION_TO_RESPONSE, FIRST_RECORD, PCT_50_RECORD, PCT_90_RECORD,
  PCT_95_RECORD, PCT_99_RECORD;

  private final String type;

  Granularity() {
    this.type = name().toLowerCase();
  }

  /**
   * All the instances of this Enum should have the same dimension name.
   * Refer {@link VeniceDimensionInterface#getDimensionName()} for more details.
   */
  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VENICE_REQUEST_GRANULARITY;
  }

  @Override
  public String getDimensionValue() {
    return type;
  }
}
