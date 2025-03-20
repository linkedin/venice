package com.linkedin.venice.stats.dimensions;

import static com.linkedin.venice.stats.VeniceOpenTelemetryMetricNamingFormat.CAMEL_CASE;
import static com.linkedin.venice.stats.VeniceOpenTelemetryMetricNamingFormat.PASCAL_CASE;
import static com.linkedin.venice.stats.VeniceOpenTelemetryMetricNamingFormat.SNAKE_CASE;
import static com.linkedin.venice.stats.VeniceOpenTelemetryMetricNamingFormat.transformMetricName;
import static com.linkedin.venice.stats.VeniceOpenTelemetryMetricNamingFormat.validateMetricName;

import com.linkedin.venice.stats.VeniceOpenTelemetryMetricNamingFormat;


public enum VeniceMetricsDimensions {
  VENICE_STORE_NAME("venice.store.name"), VENICE_CLUSTER_NAME("venice.cluster.name"),

  /** {@link com.linkedin.venice.read.RequestType} */
  VENICE_REQUEST_METHOD("venice.request.method"),

  /** {@link HttpResponseStatusEnum} ie. 200, 400, etc */
  HTTP_RESPONSE_STATUS_CODE("http.response.status_code"),

  /** {@link HttpResponseStatusCodeCategory} ie. 1xx, 2xx, etc */
  HTTP_RESPONSE_STATUS_CODE_CATEGORY("http.response.status_code_category"),

  /** {@link VeniceResponseStatusCategory} */
  VENICE_RESPONSE_STATUS_CODE_CATEGORY("venice.response.status_code_category"),

  /** {@link RequestRetryType} */
  VENICE_REQUEST_RETRY_TYPE("venice.request.retry_type"),

  /** {@link RequestRetryAbortReason} */
  VENICE_REQUEST_RETRY_ABORT_REASON("venice.request.retry_abort_reason");

  private final String[] dimensionName = new String[VeniceOpenTelemetryMetricNamingFormat.SIZE];

  VeniceMetricsDimensions(String dimensionName) {
    validateMetricName(dimensionName);
    this.dimensionName[SNAKE_CASE.getValue()] = dimensionName;
    this.dimensionName[CAMEL_CASE.getValue()] = transformMetricName(dimensionName, CAMEL_CASE);
    this.dimensionName[PASCAL_CASE.getValue()] = transformMetricName(dimensionName, PASCAL_CASE);
  }

  public String getDimensionName(VeniceOpenTelemetryMetricNamingFormat format) {
    return dimensionName[format.getValue()];
  }
}
