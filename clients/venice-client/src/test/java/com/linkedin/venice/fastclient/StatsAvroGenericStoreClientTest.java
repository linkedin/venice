package com.linkedin.venice.fastclient;

import static com.linkedin.venice.client.stats.BasicClientStats.CLIENT_METRIC_ENTITIES;
import static com.linkedin.venice.client.stats.ClientMetricEntity.ROUTE_CALL_COUNT;
import static com.linkedin.venice.client.stats.ClientMetricEntity.ROUTE_CALL_TIME;
import static com.linkedin.venice.client.stats.ClientMetricEntity.ROUTE_REQUEST_PENDING_COUNT;
import static com.linkedin.venice.client.stats.ClientMetricEntity.ROUTE_REQUEST_REJECTION_RATIO;
import static com.linkedin.venice.stats.ClientType.FAST_CLIENT;
import static com.linkedin.venice.stats.VeniceMetricsRepository.getVeniceMetricsRepository;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.stats.ClientMetricEntity;
import com.linkedin.venice.client.store.ComputeGenericRecord;
import com.linkedin.venice.client.store.streaming.StreamingCallback;
import com.linkedin.venice.compute.ComputeRequestWrapper;
import com.linkedin.venice.fastclient.meta.InstanceHealthMonitor;
import com.linkedin.venice.fastclient.utils.ClientTestUtils;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.utils.Utils;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.Metric;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class StatsAvroGenericStoreClientTest {
  private static final String CLUSTER_NAME = "test_cluster";
  private static final String ROUTE = "http://backend-1:8080";
  private static final String PENDING_ROUTE = "http://backend-2:8080";

  @DataProvider(name = "routeMetricsConfigs")
  public Object[][] routeMetricsConfigs() {
    List<Object[]> configs = new ArrayList<>();
    for (Boolean disableRouteMetrics: new Boolean[] { null, true, false }) {
      for (boolean emitOtelMetrics: new boolean[] { false, true }) {
        for (RequestType requestType: new RequestType[] { RequestType.SINGLE_GET, RequestType.MULTI_GET_STREAMING,
            RequestType.COMPUTE_STREAMING }) {
          configs.add(new Object[] { disableRouteMetrics, emitOtelMetrics, requestType });
        }
      }
    }
    return configs.toArray(new Object[0][]);
  }

  @Test(dataProvider = "routeMetricsConfigs")
  public void testRouteMetrics(Boolean disableRouteMetrics, boolean emitOtelMetrics, RequestType requestType)
      throws IOException {
    boolean routeMetricsEnabled = Boolean.FALSE.equals(disableRouteMetrics);
    String storeName = Utils.getUniqueString("route_metrics");
    InstanceHealthMonitor monitor = mock(InstanceHealthMonitor.class);
    when(monitor.getBlockedInstanceCount()).thenReturn(2);
    when(monitor.getUnhealthyInstanceCount()).thenReturn(3);
    when(monitor.getOverloadedInstanceCount()).thenReturn(4);
    when(monitor.getPendingRequestCounter(anyString())).thenReturn(5);
    when(monitor.getRejectionRatio(anyString())).thenReturn(0.25);

    try (InMemoryMetricReader reader = InMemoryMetricReader.create();
        VeniceMetricsRepository repository =
            getVeniceMetricsRepository(FAST_CLIENT, CLIENT_METRIC_ENTITIES, emitOtelMetrics, reader)) {
      ClientConfig.ClientConfigBuilder<String, String, SpecificRecord> builder =
          new ClientConfig.ClientConfigBuilder<String, String, SpecificRecord>().setStoreName(storeName)
              .setR2Client(mock(Client.class))
              .setD2Client(mock(D2Client.class))
              .setClusterDiscoveryD2Service("test_discovery")
              .setMetricsRepository(repository)
              .setInstanceHealthMonitor(monitor);
      if (disableRouteMetrics != null) {
        builder.setDisableRouteMetrics(disableRouteMetrics);
      }
      ClientConfig<String, String, SpecificRecord> config = builder.build();
      InternalAvroStoreClient<String, String> delegate = mock(InternalAvroStoreClient.class);
      StatsAvroGenericStoreClient<String, String> client = new StatsAvroGenericStoreClient<>(delegate, config);
      Set<String> keys = Collections.singleton("key");
      RequestContext requestContext;
      Runnable request;
      switch (requestType) {
        case SINGLE_GET:
          GetRequestContext<String> getContext = new GetRequestContext<>();
          when(delegate.get(getContext, "key")).thenReturn(CompletableFuture.completedFuture("value"));
          requestContext = getContext;
          request = () -> assertEquals(client.get(getContext, "key").join(), "value");
          break;
        case MULTI_GET_STREAMING:
          BatchGetRequestContext<String, String> batchContext = new BatchGetRequestContext<>(1, false);
          doAnswer(invocation -> {
            StreamingCallback<String, String> callback = invocation.getArgument(2);
            callback.onRecordReceived("key", "value");
            callback.onCompletion(Optional.empty());
            return null;
          }).when(delegate).streamingBatchGet(eq(batchContext), eq(keys), any());
          requestContext = batchContext;
          request = () -> client.streamingBatchGet(batchContext, keys, mock(StreamingCallback.class));
          break;
        case COMPUTE_STREAMING:
          ComputeRequestContext<String, String> computeContext = new ComputeRequestContext<>(1, false);
          ComputeRequestWrapper computeRequest = mock(ComputeRequestWrapper.class);
          Schema resultSchema = Schema.create(Schema.Type.STRING);
          doAnswer(invocation -> {
            StreamingCallback<String, ComputeGenericRecord> callback = invocation.getArgument(4);
            callback.onRecordReceived("key", mock(ComputeGenericRecord.class));
            callback.onCompletion(Optional.empty());
            return null;
          }).when(delegate).compute(eq(computeContext), eq(computeRequest), eq(keys), eq(resultSchema), any(), eq(0L));
          requestContext = computeContext;
          request = () -> client
              .compute(computeContext, computeRequest, keys, resultSchema, mock(StreamingCallback.class), 0L);
          break;
        default:
          throw new AssertionError("Unexpected request type: " + requestType);
      }

      CompletableFuture<Integer> pendingRoute = new CompletableFuture<>();
      requestContext.requestSentTimestampNS = System.nanoTime();
      requestContext.setServerClusterName(CLUSTER_NAME);
      requestContext.setInstanceHealthMonitor(monitor);
      requestContext.routeRequestMap.put(ROUTE, CompletableFuture.completedFuture(200));
      requestContext.routeRequestMap.put(PENDING_ROUTE, pendingRoute);
      request.run();

      assertEquals(pendingRoute.getNumberOfDependents(), routeMetricsEnabled ? 1 : 0);
      pendingRoute.completeExceptionally(new VeniceClientException("Route failed after request completion"));

      Map<String, ? extends Metric> metrics = repository.metrics();
      if (routeMetricsEnabled) {
        for (String host: new String[] { "backend-1", "backend-2" }) {
          String prefix = ClientTestUtils.getMetricPrefix(CLUSTER_NAME + "_" + host, requestType);
          assertTrue(metrics.get(prefix + "request_count.OccurrenceRate").value() > 0);
          assertEquals(metrics.get(prefix + "pending_request_count.Max").value(), 5.0);
          assertEquals(metrics.get(prefix + "rejection_ratio.Max").value(), 0.25);
          assertNotNull(metrics.get(prefix + "response_waiting_time.99thPercentile"));
          String statusMetric =
              host.equals("backend-1") ? "healthy_request_count" : "service_unavailable_request_count";
          assertTrue(metrics.get(prefix + statusMetric + ".OccurrenceRate").value() > 0);
        }
      } else {
        assertFalse(
            metrics.keySet().stream().anyMatch(name -> name.startsWith("." + CLUSTER_NAME + "_")),
            "Disabled route metrics must not register any per-host Tehuti sensors");
      }

      String requestMetricPrefix = ClientTestUtils.getMetricPrefix(storeName, requestType);
      assertTrue(metrics.get(requestMetricPrefix + "healthy_request.OccurrenceRate").value() > 0);
      assertEquals(metrics.get("." + storeName + "--blocked_instance_count.Max").value(), 2.0);
      assertEquals(metrics.get("." + storeName + "--unhealthy_instance_count.Max").value(), 3.0);
      assertEquals(metrics.get("." + storeName + "--overloaded_instance_count.Max").value(), 4.0);

      Collection<MetricData> otelMetrics = reader.collectAllMetrics();
      for (ClientMetricEntity metric: new ClientMetricEntity[] { ROUTE_CALL_COUNT, ROUTE_CALL_TIME,
          ROUTE_REQUEST_PENDING_COUNT, ROUTE_REQUEST_REJECTION_RATIO }) {
        assertEquals(
            otelMetrics.stream()
                .anyMatch(data -> data.getName().endsWith("." + metric.getMetricEntity().getMetricName())),
            routeMetricsEnabled && emitOtelMetrics,
            "Unexpected emission for " + metric);
      }
      if (emitOtelMetrics) {
        assertTrue(
            otelMetrics.stream()
                .anyMatch(data -> data.getName().endsWith(".call_count") && !data.getName().contains(".route.")));
        assertTrue(otelMetrics.stream().anyMatch(data -> data.getName().endsWith(".instance.error_count")));
        if (!routeMetricsEnabled) {
          assertFalse(otelMetrics.stream().anyMatch(data -> data.getName().contains(".route.")));
        }
      } else {
        assertTrue(otelMetrics.isEmpty());
      }
    }
  }
}
