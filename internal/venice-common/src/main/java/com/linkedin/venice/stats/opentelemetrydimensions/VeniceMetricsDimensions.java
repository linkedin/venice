package com.linkedin.venice.stats.opentelemetrydimensions;

import static com.linkedin.venice.stats.VeniceOpenTelemetryMetricFormat.CAMEL_CASE;
import static com.linkedin.venice.stats.VeniceOpenTelemetryMetricFormat.PASCAL_CASE;
import static com.linkedin.venice.stats.VeniceOpenTelemetryMetricFormat.SNAKE_CASE;
import static com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository.transformMetricName;
import static com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository.validateMetricName;

import com.linkedin.venice.stats.VeniceOpenTelemetryMetricFormat;


public enum VeniceMetricsDimensions {
  VENICE_STORE_NAME("venice.store.name"), VENICE_CLUSTER_NAME("venice.cluster.name"),

  /** {@link com.linkedin.venice.read.RequestType#requestTypeName} */
  VENICE_REQUEST_METHOD("venice.request.method"),

  /** {@link io.netty.handler.codec.http.HttpResponseStatus} ie. 200, 400, etc */
  HTTP_RESPONSE_STATUS_CODE("http.response.status_code"),

  /** {@link VeniceHttpResponseStatusCodeCategory#category} ie. 1xx, 2xx, etc */
  HTTP_RESPONSE_STATUS_CODE_CATEGORY("http.response.status_code_category"),

  /** {@link VeniceRequestValidationOutcome#outcome} */
  VENICE_REQUEST_VALIDATION_OUTCOME("venice.request.validation_outcome"),

  /** {@link VeniceResponseStatusCategory} */
  VENICE_RESPONSE_STATUS_CODE_CATEGORY("venice.response.status_code_category"),

  /** {@link VeniceRequestRetryType} */
  VENICE_REQUEST_RETRY_TYPE("venice.request.retry_type"),

  /** {@link VeniceRequestRetryAbortReason} */
  VENICE_REQUEST_RETRY_ABORT_REASON("venice.request.retry_abort_reason");

  private final String[] dimensionName = new String[VeniceOpenTelemetryMetricFormat.SIZE];

  VeniceMetricsDimensions(String dimensionName) {
    validateMetricName(dimensionName);
    this.dimensionName[SNAKE_CASE.getValue()] = dimensionName;
    this.dimensionName[CAMEL_CASE.getValue()] = transformMetricName(dimensionName, CAMEL_CASE);
    this.dimensionName[PASCAL_CASE.getValue()] = transformMetricName(dimensionName, PASCAL_CASE);
  }

  public String getDimensionName(VeniceOpenTelemetryMetricFormat format) {
    return dimensionName[format.getValue()];
  }
}
