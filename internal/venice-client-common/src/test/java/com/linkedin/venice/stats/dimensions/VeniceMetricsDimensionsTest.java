package com.linkedin.venice.stats.dimensions;

import static com.linkedin.venice.stats.VeniceOpenTelemetryMetricNamingFormat.SNAKE_CASE;
import static org.testng.Assert.assertEquals;

import com.linkedin.venice.stats.VeniceOpenTelemetryMetricNamingFormat;
import org.testng.annotations.Test;


public class VeniceMetricsDimensionsTest {
  @Test
  public void testGetDimensionNameInSnakeCase() {
    VeniceOpenTelemetryMetricNamingFormat format = SNAKE_CASE;
    for (VeniceMetricsDimensions dimension: VeniceMetricsDimensions.values()) {
      switch (dimension) {
        case VENICE_STORE_NAME:
          assertEquals(dimension.getDimensionName(format), "venice.store.name");
          break;
        case VENICE_CLUSTER_NAME:
          assertEquals(dimension.getDimensionName(format), "venice.cluster.name");
          break;
        case VENICE_REQUEST_METHOD:
          assertEquals(dimension.getDimensionName(format), "venice.request.method");
          break;
        case HTTP_RESPONSE_STATUS_CODE:
          assertEquals(dimension.getDimensionName(format), "http.response.status_code");
          break;
        case HTTP_RESPONSE_STATUS_CODE_CATEGORY:
          assertEquals(dimension.getDimensionName(format), "http.response.status_code_category");
          break;
        case VENICE_RESPONSE_STATUS_CODE_CATEGORY:
          assertEquals(dimension.getDimensionName(format), "venice.response.status_code_category");
          break;
        case VENICE_REQUEST_RETRY_TYPE:
          assertEquals(dimension.getDimensionName(format), "venice.request.retry_type");
          break;
        case VENICE_REQUEST_RETRY_ABORT_REASON:
          assertEquals(dimension.getDimensionName(format), "venice.request.retry_abort_reason");
          break;
        default:
          throw new IllegalArgumentException("Unknown dimension: " + dimension);
      }
    }
  }

  @Test
  public void testGetDimensionNameInCamelCase() {
    VeniceOpenTelemetryMetricNamingFormat format = VeniceOpenTelemetryMetricNamingFormat.CAMEL_CASE;
    for (VeniceMetricsDimensions dimension: VeniceMetricsDimensions.values()) {
      switch (dimension) {
        case VENICE_STORE_NAME:
          assertEquals(dimension.getDimensionName(format), "venice.store.name");
          break;
        case VENICE_CLUSTER_NAME:
          assertEquals(dimension.getDimensionName(format), "venice.cluster.name");
          break;
        case VENICE_REQUEST_METHOD:
          assertEquals(dimension.getDimensionName(format), "venice.request.method");
          break;
        case HTTP_RESPONSE_STATUS_CODE:
          assertEquals(dimension.getDimensionName(format), "http.response.statusCode");
          break;
        case HTTP_RESPONSE_STATUS_CODE_CATEGORY:
          assertEquals(dimension.getDimensionName(format), "http.response.statusCodeCategory");
          break;
        case VENICE_RESPONSE_STATUS_CODE_CATEGORY:
          assertEquals(dimension.getDimensionName(format), "venice.response.statusCodeCategory");
          break;
        case VENICE_REQUEST_RETRY_TYPE:
          assertEquals(dimension.getDimensionName(format), "venice.request.retryType");
          break;
        case VENICE_REQUEST_RETRY_ABORT_REASON:
          assertEquals(dimension.getDimensionName(format), "venice.request.retryAbortReason");
          break;
        default:
          throw new IllegalArgumentException("Unknown dimension: " + dimension);
      }
    }
  }

  @Test
  public void testGetDimensionNameInPascalCase() {
    VeniceOpenTelemetryMetricNamingFormat format = VeniceOpenTelemetryMetricNamingFormat.PASCAL_CASE;
    for (VeniceMetricsDimensions dimension: VeniceMetricsDimensions.values()) {
      switch (dimension) {
        case VENICE_STORE_NAME:
          assertEquals(dimension.getDimensionName(format), "Venice.Store.Name");
          break;
        case VENICE_CLUSTER_NAME:
          assertEquals(dimension.getDimensionName(format), "Venice.Cluster.Name");
          break;
        case VENICE_REQUEST_METHOD:
          assertEquals(dimension.getDimensionName(format), "Venice.Request.Method");
          break;
        case HTTP_RESPONSE_STATUS_CODE:
          assertEquals(dimension.getDimensionName(format), "Http.Response.StatusCode");
          break;
        case HTTP_RESPONSE_STATUS_CODE_CATEGORY:
          assertEquals(dimension.getDimensionName(format), "Http.Response.StatusCodeCategory");
          break;
        case VENICE_RESPONSE_STATUS_CODE_CATEGORY:
          assertEquals(dimension.getDimensionName(format), "Venice.Response.StatusCodeCategory");
          break;
        case VENICE_REQUEST_RETRY_TYPE:
          assertEquals(dimension.getDimensionName(format), "Venice.Request.RetryType");
          break;
        case VENICE_REQUEST_RETRY_ABORT_REASON:
          assertEquals(dimension.getDimensionName(format), "Venice.Request.RetryAbortReason");
          break;
        default:
          throw new IllegalArgumentException("Unknown dimension: " + dimension);
      }
    }
  }
}
