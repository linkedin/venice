package com.linkedin.venice.stats;

import com.linkedin.venice.utils.VeniceEnumValue;


public enum VeniceOpenTelemetryMetricFormat implements VeniceEnumValue {
  /**
   * Default format if not configured, names are defined as per this.
   * should use snake case as per https://opentelemetry.io/docs/specs/semconv/general/attribute-naming/
   * For example: http.response.status_code
   */
  SNAKE_CASE(0),
  /**
   * Alternate format for attribute names. If configured, defined names in snake_case will be
   * transformed to either one of below formats.
   *
   * camel case: For example, http.response.statusCode
   * pascal case: For example, Http.Response.StatusCode
   */
  CAMEL_CASE(1), PASCAL_CASE(2);

  private final int value;

  VeniceOpenTelemetryMetricFormat(int value) {
    this.value = value;
  }

  public static final int SIZE = values().length;

  @Override
  public int getValue() {
    return value;
  }
}
