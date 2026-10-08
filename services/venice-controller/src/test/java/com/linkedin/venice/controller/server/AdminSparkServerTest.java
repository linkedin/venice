package com.linkedin.venice.controller.server;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.CLUSTER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.STORE_NAME;
import static com.linkedin.venice.controllerapi.ControllerRoute.HEALTH;
import static com.linkedin.venice.controllerapi.ControllerRoute.STORE;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.ControllerRoute;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.status.protocol.PushJobDetails;
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

  @Test(timeOut = 30000)
  public void testHealthHttpAndDisabledRoutePolicy() throws Exception {
    Admin admin = mock(Admin.class);
    when(admin.getLogContext()).thenReturn(LogContext.EMPTY);
    VeniceControllerRequestHandler requestHandler = mock(VeniceControllerRequestHandler.class);
    when(requestHandler.getStoreRequestHandler()).thenReturn(mock(StoreRequestHandler.class));
    when(requestHandler.getSchemaRequestHandler()).thenReturn(mock(SchemaRequestHandler.class));
    when(requestHandler.getClusterAdminOpsRequestHandler()).thenReturn(mock(ClusterAdminOpsRequestHandler.class));
    PubSubTopicRepository pubSubTopicRepository = mock(PubSubTopicRepository.class);
    List<ControllerRoute> disabledRoutes = new CopyOnWriteArrayList<>();
    MetricsRepository metricsRepository = new MetricsRepository();
    try (
        InternalAvroSpecificSerializer<PushJobDetails> serializer =
            AvroProtocolDefinition.PUSH_JOB_DETAILS.getSerializer();
        AdminSparkServer server = new AdminSparkServer(
            TestUtils.getFreePort(),
            admin,
            metricsRepository,
            Collections.emptySet(),
            true,
            Optional.empty(),
            false,
            Optional.empty(),
            disabledRoutes,
            VeniceProperties.empty(),
            false,
            pubSubTopicRepository,
            requestHandler,
            serializer,
            () -> true);
        CloseableHttpClient client = HttpClients.custom()
            .setDefaultRequestConfig(RequestConfig.custom().setConnectTimeout(5000).setSocketTimeout(5000).build())
            .disableAutomaticRetries()
            .disableRedirectHandling()
            .build()) {
      server.start();
      clearInvocations(admin, requestHandler, pubSubTopicRepository);
      String controllerUrl = "http://localhost:" + server.getPort();
      assertHttpResponse(client, new HttpGet(controllerUrl + HEALTH.getPath()), HttpStatus.SC_OK, "OK");
      assertHttpResponse(
          client,
          new HttpGet(controllerUrl + STORE.getPath() + "?" + CLUSTER + "=test_cluster&" + STORE_NAME + "=test_store"),
          HttpStatus.SC_FORBIDDEN,
          "Access denied, Venice Controller has enforced SSL.");
      assertHttpResponse(
          client,
          new HttpGet(controllerUrl + "/health/store"),
          HttpStatus.SC_FORBIDDEN,
          "Access denied, Venice Controller has enforced SSL.");
      assertHttpResponse(client, new HttpPost(controllerUrl + HEALTH.getPath()), HttpStatus.SC_NOT_FOUND, null);

      disabledRoutes.add(HEALTH);
      assertHttpResponse(
          client,
          new HttpGet(controllerUrl + HEALTH.getPath()),
          HttpStatus.SC_FORBIDDEN,
          "Route /health has been disabled in venice controller config!!");
      verifyNoInteractions(admin, requestHandler, pubSubTopicRepository);
    } finally {
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
      if (expectedStatus == HttpStatus.SC_OK) {
        assertEquals(ContentType.get(response.getEntity()).getMimeType(), HttpConstants.TEXT_PLAIN);
      }
    }
  }
}
