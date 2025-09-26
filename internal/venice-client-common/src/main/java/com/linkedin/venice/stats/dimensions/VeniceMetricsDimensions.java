package com.linkedin.venice.stats.dimensions;

import static com.linkedin.venice.stats.VeniceOpenTelemetryMetricNamingFormat.CAMEL_CASE;
import static com.linkedin.venice.stats.VeniceOpenTelemetryMetricNamingFormat.PASCAL_CASE;
import static com.linkedin.venice.stats.VeniceOpenTelemetryMetricNamingFormat.SNAKE_CASE;
import static com.linkedin.venice.stats.VeniceOpenTelemetryMetricNamingFormat.transformMetricName;
import static com.linkedin.venice.stats.VeniceOpenTelemetryMetricNamingFormat.validateMetricName;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricNamingFormat;


public enum VeniceMetricsDimensions {
  VENICE_STORE_NAME("venice.store.name"), VENICE_CLUSTER_NAME("venice.cluster.name"),

  /** {@link com.linkedin.venice.read.RequestType} */
  VENICE_REQUEST_METHOD("venice.request.method"),

  /** Route name for routing metrics */
  VENICE_ROUTE_NAME("venice.route.name"),

  /** {@link HttpResponseStatusEnum} ie. 200, 400, etc */
  HTTP_RESPONSE_STATUS_CODE("http.response.status_code"),

  /** {@link HttpResponseStatusCodeCategory} ie. 1xx, 2xx, etc */
  HTTP_RESPONSE_STATUS_CODE_CATEGORY("http.response.status_code_category"),

  /** {@link VeniceResponseStatusCategory} */
  VENICE_RESPONSE_STATUS_CODE_CATEGORY("venice.response.status_code_category"),

  /** {@link RequestRetryType} */
  VENICE_REQUEST_RETRY_TYPE("venice.request.retry_type"),

  /** {@link com.linkedin.venice.stats.dimensions.MessageType} */
  VENICE_MESSAGE_TYPE("venice.message.type"),

  /** Fanout type for requests {@link com.linkedin.venice.stats.dimensions.RequestFanoutType} (e.g., original vs retry) */
  VENICE_REQUEST_FANOUT_TYPE("venice.request.fanout_type"),

  /** {@link com.linkedin.venice.stats.dimensions.RejectionReason} */
  VENICE_REQUEST_REJECTION_REASON("venice.request.rejection_reason"),

  /**
   * {@link StreamProgress} Streaming delivery progress for batch responses
   * (e.g., first, 50pct, 90pct, etc.)
   */
  VENICE_STREAM_PROGRESS("venice.stream.progress"),

  /** {@link RequestRetryAbortReason} */
  VENICE_REQUEST_RETRY_ABORT_REASON("venice.request.retry_abort_reason"),

  /** {@link RepushStoreTriggerSource} */
  REPUSH_TRIGGER_SOURCE("repush.trigger.source"),

  /** Instance error type for blocked, unhealthy, and overloaded instances */
  VENICE_INSTANCE_ERROR_TYPE("venice.instance.error_type");

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

  // This is only for testing purpose and should never be used in production code.
  @VisibleForTesting
  public String getDimensionNameInDefaultFormat() {
    return dimensionName[VeniceOpenTelemetryMetricNamingFormat.getDefaultFormat().getValue()];
  }
}
