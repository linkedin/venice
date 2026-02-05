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
        case VENICE_CONTROLLER_ENDPOINT:
          assertEquals(dimension.getDimensionName(format), "venice.controller.endpoint");
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
        case VENICE_MESSAGE_TYPE:
          assertEquals(dimension.getDimensionName(format), "venice.message.type");
          break;
        case VENICE_STREAM_PROGRESS:
          assertEquals(dimension.getDimensionName(format), "venice.stream.progress");
          break;
        case STORE_REPUSH_TRIGGER_SOURCE:
          assertEquals(dimension.getDimensionName(format), "store.repush.trigger.source");
          break;
        case VENICE_ROUTE_NAME:
          assertEquals(dimension.getDimensionName(format), "venice.route.name");
          break;
        case VENICE_REQUEST_REJECTION_REASON:
          assertEquals(dimension.getDimensionName(format), "venice.request.rejection_reason");
          break;
        case VENICE_REQUEST_FANOUT_TYPE:
          assertEquals(dimension.getDimensionName(format), "venice.request.fanout_type");
          break;
        case VENICE_INSTANCE_ERROR_TYPE:
          assertEquals(dimension.getDimensionName(format), "venice.instance.error_type");
          break;
        case VENICE_HELIX_GROUP_ID:
          assertEquals(dimension.getDimensionName(format), "venice.helix_group.id");
          break;
        case VENICE_REGION_NAME:
          assertEquals(dimension.getDimensionName(format), "venice.region.name");
          break;
        case VENICE_VERSION_ROLE:
          assertEquals(dimension.getDimensionName(format), "venice.version.role");
          break;
        case VENICE_REPLICA_TYPE:
          assertEquals(dimension.getDimensionName(format), "venice.replica.type");
          break;
        case VENICE_REPLICA_STATE:
          assertEquals(dimension.getDimensionName(format), "venice.replica.state");
          break;
        case VENICE_DCR_EVENT:
          assertEquals(dimension.getDimensionName(format), "venice.dcr.event");
          break;
        case VENICE_REGION_LOCALITY:
          assertEquals(dimension.getDimensionName(format), "venice.region.locality");
          break;
        case VENICE_SOURCE_REGION:
          assertEquals(dimension.getDimensionName(format), "venice.source.region");
          break;
        case VENICE_DESTINATION_REGION:
          assertEquals(dimension.getDimensionName(format), "venice.destination.region");
          break;
        case VENICE_INGESTION_SOURCE_COMPONENT:
          assertEquals(dimension.getDimensionName(format), "venice.ingestion.source.component");
          break;
        case VENICE_INGESTION_DESTINATION_COMPONENT:
          assertEquals(dimension.getDimensionName(format), "venice.ingestion.destination.component");
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
        case VENICE_CONTROLLER_ENDPOINT:
          assertEquals(dimension.getDimensionName(format), "venice.controller.endpoint");
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
        case VENICE_MESSAGE_TYPE:
          assertEquals(dimension.getDimensionName(format), "venice.message.type");
          break;
        case VENICE_STREAM_PROGRESS:
          assertEquals(dimension.getDimensionName(format), "venice.stream.progress");
          break;
        case STORE_REPUSH_TRIGGER_SOURCE:
          assertEquals(dimension.getDimensionName(format), "store.repush.trigger.source");
          break;
        case VENICE_ROUTE_NAME:
          assertEquals(dimension.getDimensionName(format), "venice.route.name");
          break;
        case VENICE_REQUEST_REJECTION_REASON:
          assertEquals(dimension.getDimensionName(format), "venice.request.rejectionReason");
          break;
        case VENICE_REQUEST_FANOUT_TYPE:
          assertEquals(dimension.getDimensionName(format), "venice.request.fanoutType");
          break;
        case VENICE_INSTANCE_ERROR_TYPE:
          assertEquals(dimension.getDimensionName(format), "venice.instance.errorType");
          break;
        case VENICE_HELIX_GROUP_ID:
          assertEquals(dimension.getDimensionName(format), "venice.helixGroup.id");
          break;
        case VENICE_REGION_NAME:
          assertEquals(dimension.getDimensionName(format), "venice.region.name");
          break;
        case VENICE_VERSION_ROLE:
          assertEquals(dimension.getDimensionName(format), "venice.version.role");
          break;
        case VENICE_REPLICA_TYPE:
          assertEquals(dimension.getDimensionName(format), "venice.replica.type");
          break;
        case VENICE_REPLICA_STATE:
          assertEquals(dimension.getDimensionName(format), "venice.replica.state");
          break;
        case VENICE_DCR_EVENT:
          assertEquals(dimension.getDimensionName(format), "venice.dcr.event");
          break;
        case VENICE_REGION_LOCALITY:
          assertEquals(dimension.getDimensionName(format), "venice.region.locality");
          break;
        case VENICE_SOURCE_REGION:
          assertEquals(dimension.getDimensionName(format), "venice.source.region");
          break;
        case VENICE_DESTINATION_REGION:
          assertEquals(dimension.getDimensionName(format), "venice.destination.region");
          break;
        case VENICE_INGESTION_SOURCE_COMPONENT:
          assertEquals(dimension.getDimensionName(format), "venice.ingestion.source.component");
          break;
        case VENICE_INGESTION_DESTINATION_COMPONENT:
          assertEquals(dimension.getDimensionName(format), "venice.ingestion.destination.component");
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
        case VENICE_CONTROLLER_ENDPOINT:
          assertEquals(dimension.getDimensionName(format), "Venice.Controller.Endpoint");
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
        case VENICE_MESSAGE_TYPE:
          assertEquals(dimension.getDimensionName(format), "Venice.Message.Type");
          break;
        case VENICE_STREAM_PROGRESS:
          assertEquals(dimension.getDimensionName(format), "Venice.Stream.Progress");
          break;
        case STORE_REPUSH_TRIGGER_SOURCE:
          assertEquals(dimension.getDimensionName(format), "Store.Repush.Trigger.Source");
          break;
        case VENICE_ROUTE_NAME:
          assertEquals(dimension.getDimensionName(format), "Venice.Route.Name");
          break;
        case VENICE_REQUEST_REJECTION_REASON:
          assertEquals(dimension.getDimensionName(format), "Venice.Request.RejectionReason");
          break;
        case VENICE_REQUEST_FANOUT_TYPE:
          assertEquals(dimension.getDimensionName(format), "Venice.Request.FanoutType");
          break;
        case VENICE_INSTANCE_ERROR_TYPE:
          assertEquals(dimension.getDimensionName(format), "Venice.Instance.ErrorType");
          break;
        case VENICE_HELIX_GROUP_ID:
          assertEquals(dimension.getDimensionName(format), "Venice.HelixGroup.Id");
          break;
        case VENICE_REGION_NAME:
          assertEquals(dimension.getDimensionName(format), "Venice.Region.Name");
          break;
        case VENICE_VERSION_ROLE:
          assertEquals(dimension.getDimensionName(format), "Venice.Version.Role");
          break;
        case VENICE_REPLICA_TYPE:
          assertEquals(dimension.getDimensionName(format), "Venice.Replica.Type");
          break;
        case VENICE_REPLICA_STATE:
          assertEquals(dimension.getDimensionName(format), "Venice.Replica.State");
          break;
        case VENICE_DCR_EVENT:
          assertEquals(dimension.getDimensionName(format), "Venice.Dcr.Event");
          break;
        case VENICE_REGION_LOCALITY:
          assertEquals(dimension.getDimensionName(format), "Venice.Region.Locality");
          break;
        case VENICE_SOURCE_REGION:
          assertEquals(dimension.getDimensionName(format), "Venice.Source.Region");
          break;
        case VENICE_DESTINATION_REGION:
          assertEquals(dimension.getDimensionName(format), "Venice.Destination.Region");
          break;
        case VENICE_INGESTION_SOURCE_COMPONENT:
          assertEquals(dimension.getDimensionName(format), "Venice.Ingestion.Source.Component");
          break;
        case VENICE_INGESTION_DESTINATION_COMPONENT:
          assertEquals(dimension.getDimensionName(format), "Venice.Ingestion.Destination.Component");
          break;
        default:
          throw new IllegalArgumentException("Unknown dimension: " + dimension);
      }
    }
  }
}
