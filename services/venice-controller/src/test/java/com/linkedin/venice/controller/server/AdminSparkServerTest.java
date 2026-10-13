package com.linkedin.venice.controller.server;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.CLUSTER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.STORE_NAME;
import static com.linkedin.venice.controllerapi.ControllerRoute.HEALTH;
import static com.linkedin.venice.controllerapi.ControllerRoute.LEADER_CONTROLLER;
import static com.linkedin.venice.controllerapi.ControllerRoute.STORE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.ControllerRoute;
import com.linkedin.venice.protocols.controller.LeaderControllerGrpcResponse;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.status.protocol.PushJobDetails;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.InMemoryLogAppender;
import com.linkedin.venice.utils.LogContext;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.filter.LevelRangeFilter;
import org.testng.annotations.Test;
import spark.Request;
import spark.Response;


public class AdminSparkServerTest {
  @Test
  public void testHealthResponseAndFailureMarker() throws Exception {
    for (boolean ready: new boolean[] { false, true }) {
      Request request = mock(Request.class);
      Response response = mock(Response.class);
      assertEquals(AdminSparkServer.healthRoute(() -> ready).handle(request, response), ready ? "OK" : "NOT_READY");
      verify(response).type(HttpConstants.TEXT_PLAIN);
      verify(response).status(ready ? HttpStatus.SC_OK : HttpStatus.SC_SERVICE_UNAVAILABLE);
      verify(request, ready ? never() : times(1)).attribute("succeed", false);
      verifyNoMoreInteractions(request, response);
    }
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class, timeOut = 30000)
  public void testHealthHttpAuditMetricsAndDisabledRoutePolicy(boolean enforceSSL) throws Exception {
    Admin admin = mock(Admin.class);
    when(admin.getLogContext()).thenReturn(LogContext.EMPTY);
    VeniceControllerRequestHandler requestHandler = mock(VeniceControllerRequestHandler.class);
    when(requestHandler.getStoreRequestHandler()).thenReturn(mock(StoreRequestHandler.class));
    when(requestHandler.getSchemaRequestHandler()).thenReturn(mock(SchemaRequestHandler.class));
    when(requestHandler.getClusterAdminOpsRequestHandler()).thenReturn(mock(ClusterAdminOpsRequestHandler.class));
    PubSubTopicRepository pubSubTopicRepository = mock(PubSubTopicRepository.class);
    List<ControllerRoute> disabledRoutes = new CopyOnWriteArrayList<>();
    MetricsRepository metricsRepository = new MetricsRepository();
    AtomicBoolean ready = new AtomicBoolean(true);
    Logger logger = (Logger) LogManager.getLogger(AdminSparkServer.class);
    InMemoryLogAppender logAppender = new InMemoryLogAppender.Builder().build();
    logAppender
        .addFilter(LevelRangeFilter.createFilter(Level.INFO, Level.INFO, Filter.Result.ACCEPT, Filter.Result.DENY));
    logAppender.start();
    logger.addAppender(logAppender);
    try (
        InternalAvroSpecificSerializer<PushJobDetails> serializer =
            AvroProtocolDefinition.PUSH_JOB_DETAILS.getSerializer();
        AdminSparkServer server = new AdminSparkServer(
            TestUtils.getFreePort(),
            admin,
            metricsRepository,
            Collections.emptySet(),
            enforceSSL,
            Optional.empty(),
            false,
            Optional.empty(),
            disabledRoutes,
            VeniceProperties.empty(),
            false,
            pubSubTopicRepository,
            requestHandler,
            serializer,
            ready::get);
        CloseableHttpClient client = HttpClients.custom()
            .setDefaultRequestConfig(RequestConfig.custom().setConnectTimeout(5000).setSocketTimeout(5000).build())
            .disableAutomaticRetries()
            .disableRedirectHandling()
            .build()) {
      server.start();
      clearInvocations(admin, requestHandler, pubSubTopicRepository);
      logAppender.getLogs().clear();
      String controllerUrl = "http://localhost:" + server.getPort();
      assertHttpResponse(client, new HttpGet(controllerUrl + HEALTH.getPath()), HttpStatus.SC_OK, "OK");
      ready.set(false);
      assertHttpResponse(
          client,
          new HttpGet(controllerUrl + HEALTH.getPath()),
          HttpStatus.SC_SERVICE_UNAVAILABLE,
          "NOT_READY");
      assertTrue(logAppender.getLogs().isEmpty(), "Health probes must not produce INFO audit logs");
      String metricPrefix = "._controller_spark_server--";
      assertEquals(metricsRepository.getMetric(metricPrefix + "request.Count").value(), 2D);
      assertEquals(metricsRepository.getMetric(metricPrefix + "finished_request.Count").value(), 2D);
      assertEquals(metricsRepository.getMetric(metricPrefix + "current_in_flight_request.Total").value(), 0D);
      assertEquals(metricsRepository.getMetric(metricPrefix + "successful_request.Count").value(), 1D);
      assertEquals(metricsRepository.getMetric(metricPrefix + "failed_request.Count").value(), 1D);
      assertTrue(metricsRepository.getMetric(metricPrefix + "successful_request_latency.50thPercentile").value() >= 0);
      assertTrue(metricsRepository.getMetric(metricPrefix + "failed_request_latency.50thPercentile").value() >= 0);
      if (enforceSSL) {
        assertHttpResponse(
            client,
            new HttpGet(
                controllerUrl + STORE.getPath() + "?" + CLUSTER + "=test_cluster&" + STORE_NAME + "=test_store"),
            HttpStatus.SC_FORBIDDEN,
            "Access denied, Venice Controller has enforced SSL.");
        assertHttpResponse(
            client,
            new HttpGet(controllerUrl + "/health/store"),
            HttpStatus.SC_FORBIDDEN,
            "Access denied, Venice Controller has enforced SSL.");
        assertHttpResponse(
            client,
            new HttpGet(controllerUrl + "/health/"),
            HttpStatus.SC_FORBIDDEN,
            "Access denied, Venice Controller has enforced SSL.");
      }
      HttpUriRequest[] nonProbes =
          { new HttpPost(controllerUrl + HEALTH.getPath()), new HttpGet(controllerUrl + "/he/alth") };
      for (HttpUriRequest request: nonProbes) {
        logAppender.getLogs().clear();
        assertHttpResponse(
            client,
            request,
            enforceSSL ? HttpStatus.SC_FORBIDDEN : HttpStatus.SC_NOT_FOUND,
            enforceSSL ? "Access denied, Venice Controller has enforced SSL." : null);
        assertTrue(logAppender.getLogs().get(0).startsWith("[AUDIT] " + request.getMethod() + " "));
        if (!enforceSSL) {
          assertEquals(logAppender.getLogs().size(), 2, "Non-probe requests retain both audit records");
        }
      }

      disabledRoutes.add(HEALTH);
      assertHttpResponse(
          client,
          new HttpGet(controllerUrl + HEALTH.getPath()),
          HttpStatus.SC_FORBIDDEN,
          "Route /health has been disabled in venice controller config!!");
      verifyNoInteractions(admin, requestHandler, pubSubTopicRepository);
      logAppender.getLogs().clear();
      assertHttpResponse(
          client,
          new HttpGet(controllerUrl + LEADER_CONTROLLER.getPath()),
          HttpStatus.SC_BAD_REQUEST,
          null);
      assertEquals(logAppender.getLogs().size(), 2);
      assertTrue(logAppender.getLogs().get(0).startsWith("[AUDIT] GET "));
      assertTrue(logAppender.getLogs().get(1).contains("HttpStatus: 400"));
      when(requestHandler.getLeaderControllerDetails(any())).thenReturn(
          LeaderControllerGrpcResponse.newBuilder()
              .setClusterName("test_cluster")
              .setHttpUrl("http://localhost")
              .build());
      logAppender.getLogs().clear();
      assertHttpResponse(
          client,
          new HttpGet(controllerUrl + LEADER_CONTROLLER.getPath() + "?" + CLUSTER + "=test_cluster"),
          HttpStatus.SC_OK,
          null);
      assertEquals(logAppender.getLogs().size(), 2);
      assertTrue(logAppender.getLogs().get(0).startsWith("[AUDIT] GET "));
      assertTrue(logAppender.getLogs().get(1).startsWith("[AUDIT] SUCCESS "));
      verify(admin, times(2)).getParentControllerRegionState();
      verify(admin, times(2)).isParent();
      verifyNoMoreInteractions(admin);
      verify(requestHandler).getLeaderControllerDetails(any());
      verifyNoMoreInteractions(requestHandler);
      verifyNoInteractions(pubSubTopicRepository);
    } finally {
      logger.removeAppender(logAppender);
      logAppender.stop();
      metricsRepository.close();
    }
  }

  private void assertHttpResponse(
      CloseableHttpClient client,
      HttpUriRequest request,
      int expectedStatus,
      String expectedBody) throws IOException {
    try (CloseableHttpResponse response = client.execute(request)) {
      assertEquals(response.getStatusLine().getStatusCode(), expectedStatus);
      if (expectedBody != null) {
        assertEquals(EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8), expectedBody);
      }
      if ("OK".equals(expectedBody) || "NOT_READY".equals(expectedBody)) {
        assertEquals(ContentType.get(response.getEntity()).getMimeType(), HttpConstants.TEXT_PLAIN);
      }
    }
  }
}
