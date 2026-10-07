package com.linkedin.venice.controller.server;

import static com.linkedin.venice.CommonConfigKeys.SSL_NEEDS_CLIENT_CERT;
import static com.linkedin.venice.controllerapi.ControllerRoute.HEALTH;
import static com.linkedin.venice.controllerapi.ControllerRoute.NEW_STORE;
import static com.linkedin.venice.controllerapi.ControllerRoute.STORE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CONTROLLER_ENDPOINT;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.SSLConfig;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.stats.SparkServerStats;
import com.linkedin.venice.controllerapi.ControllerRoute;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.dimensions.HttpResponseStatusCodeCategory;
import com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.LogContext;
import com.linkedin.venice.utils.OpenTelemetryDataTestUtils;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.VeniceProperties;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.metrics.MetricsRepository;
import java.io.File;
import java.io.IOException;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.util.EntityUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class AdminSparkServerTest {
  private static final RequestConfig REQUEST_CONFIG =
      RequestConfig.custom().setConnectTimeout(5000).setConnectionRequestTimeout(5000).setSocketTimeout(5000).build();

  private Admin admin;
  private VeniceControllerRequestHandler requestHandler;
  private DynamicAccessController accessController;
  private PubSubTopicRepository pubSubTopicRepository;
  private MetricsRepository metricsRepository;

  @BeforeMethod
  public void setUp() {
    admin = mock(Admin.class);
    when(admin.getLogContext()).thenReturn(LogContext.EMPTY);
    requestHandler = mock(VeniceControllerRequestHandler.class);
    when(requestHandler.getStoreRequestHandler()).thenReturn(mock(StoreRequestHandler.class));
    when(requestHandler.getSchemaRequestHandler()).thenReturn(mock(SchemaRequestHandler.class));
    when(requestHandler.getClusterAdminOpsRequestHandler()).thenReturn(mock(ClusterAdminOpsRequestHandler.class));
    accessController = mock(DynamicAccessController.class);
    pubSubTopicRepository = mock(PubSubTopicRepository.class);
    metricsRepository = new MetricsRepository();
  }

  @AfterMethod(alwaysRun = true)
  public void tearDown() {
    metricsRepository.close();
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class, timeOut = 30000)
  public void testHttpHealthFollowsOwnerReadiness(boolean enforceSSL) throws IOException {
    AtomicBoolean ready = new AtomicBoolean();
    try (AdminSparkServer server = createServer(enforceSSL, Optional.empty(), Collections.emptyList(), ready::get);
        CloseableHttpClient client = createClient(Optional.empty())) {
      startServer(server);
      String controllerUrl = "http://localhost:" + server.getPort();
      assertHealthEndpoint(client, controllerUrl, HttpStatus.SC_SERVICE_UNAVAILABLE, "NOT_READY");
      ready.set(true);
      assertHealthEndpoint(client, controllerUrl);
      ready.set(false);
      assertHealthEndpoint(client, controllerUrl, HttpStatus.SC_SERVICE_UNAVAILABLE, "NOT_READY");
      verifyNoInteractions(admin, requestHandler, accessController, pubSubTopicRepository);
    }
  }

  @Test(timeOut = 30000)
  public void testLegacyConstructorDoesNotAssumeReadiness() throws IOException {
    try (AdminSparkServer server = new AdminSparkServer(
        TestUtils.getFreePort(),
        admin,
        metricsRepository,
        Collections.emptySet(),
        true,
        Optional.empty(),
        false,
        Optional.of(accessController),
        Collections.emptyList(),
        VeniceProperties.empty(),
        false,
        pubSubTopicRepository,
        requestHandler); CloseableHttpClient client = createClient(Optional.empty())) {
      startServer(server);
      assertHealthEndpoint(
          client,
          "http://localhost:" + server.getPort(),
          HttpStatus.SC_SERVICE_UNAVAILABLE,
          "NOT_READY");
      verifyNoInteractions(admin, requestHandler, accessController, pubSubTopicRepository);
    }
  }

  @Test
  public void testNullReadinessSignalIsRejected() {
    expectThrows(
        NullPointerException.class,
        () -> createServer(false, Optional.empty(), Collections.emptyList(), null));
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class, timeOut = 30000)
  public void testHealthHonorsDisabledRoutes(boolean sslEnabled) throws IOException {
    Optional<SSLConfig> sslConfig = sslEnabled
        ? Optional.of(new SSLConfig(new VeniceProperties(SslUtils.getVeniceLocalSslProperties())))
        : Optional.empty();
    Optional<SSLFactory> sslFactory = sslEnabled ? Optional.of(SslUtils.getVeniceLocalSslFactory()) : Optional.empty();
    AtomicBoolean ready = new AtomicBoolean();
    AtomicInteger readinessChecks = new AtomicInteger();

    try (AdminSparkServer server = createServer(true, sslConfig, Collections.singletonList(HEALTH), () -> {
      readinessChecks.incrementAndGet();
      return ready.get();
    }); CloseableHttpClient client = createClient(sslFactory)) {
      startServer(server);
      String healthUrl = (sslEnabled ? "https" : "http") + "://localhost:" + server.getPort() + HEALTH.getPath();
      for (boolean readiness: new boolean[] { false, true }) {
        ready.set(readiness);
        try (CloseableHttpResponse response = client.execute(new HttpGet(healthUrl))) {
          assertEquals(response.getStatusLine().getStatusCode(), HttpStatus.SC_FORBIDDEN);
          assertEquals(
              EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8),
              "Route /health has been disabled in venice controller config!!");
        }
      }
      assertEquals(readinessChecks.get(), 0);
      verifyNoInteractions(admin, requestHandler, accessController, pubSubTopicRepository);
    }
  }

  @Test(timeOut = 30000)
  public void testHttpHealthDoesNotRelaxSslForOtherRoutes() throws IOException {
    try (AdminSparkServer server = createServer(true, Optional.empty(), Collections.emptyList());
        CloseableHttpClient client = createClient(Optional.empty())) {
      startServer(server);
      String controllerUrl = "http://localhost:" + server.getPort();
      HttpUriRequest[] requests =
          { new HttpGet(controllerUrl + STORE.getPath() + "?cluster=test_cluster&name=test_store"),
              new HttpPost(controllerUrl + NEW_STORE.getPath()), new HttpGet(controllerUrl + "/health/store"),
              new HttpGet(controllerUrl + "/healthcheck") };
      for (HttpUriRequest request: requests) {
        try (CloseableHttpResponse response = client.execute(request)) {
          assertEquals(response.getStatusLine().getStatusCode(), HttpStatus.SC_FORBIDDEN);
          assertEquals(
              EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8),
              "Access denied, Venice Controller has enforced SSL.");
        }
      }
      assertHealthEndpoint(client, controllerUrl);
      verifyNoInteractions(admin, requestHandler, accessController, pubSubTopicRepository);
    }
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class, timeOut = 30000)
  public void testHttpsHealthRespectsClientCertificatePolicy(boolean needsClientCert) throws Exception {
    Properties sslProperties = SslUtils.getVeniceLocalSslProperties();
    sslProperties.setProperty(SSL_NEEDS_CLIENT_CERT, Boolean.toString(needsClientCert));
    SSLConfig sslConfig = new SSLConfig(new VeniceProperties(sslProperties));

    try (AdminSparkServer server = createServer(true, Optional.of(sslConfig), Collections.emptyList());
        CloseableHttpClient client = createClient(Optional.of(SslUtils.getVeniceLocalSslFactory()))) {
      startServer(server);
      String controllerUrl = "https://localhost:" + server.getPort();
      assertHealthEndpoint(client, controllerUrl);

      SSLContext trustOnlyContext = SSLContextBuilder.create()
          .loadTrustMaterial(
              new File(sslConfig.getSslTrustStoreLocation()),
              sslConfig.getSslTrustStorePassword().toCharArray())
          .build();
      try (CloseableHttpClient clientWithoutCertificate = HttpClients.custom()
          .setDefaultRequestConfig(REQUEST_CONFIG)
          .setSSLContext(trustOnlyContext)
          .disableAutomaticRetries()
          .disableRedirectHandling()
          .build()) {
        if (needsClientCert) {
          IOException rejection = expectThrows(IOException.class, () -> {
            try (CloseableHttpResponse response =
                clientWithoutCertificate.execute(new HttpGet(controllerUrl + HEALTH.getPath()))) {
              assertEquals(response.getStatusLine().getStatusCode(), HttpStatus.SC_OK);
            }
          });
          // A missing client certificate can surface as a TLS alert or a peer-closed socket.
          assertTrue(
              rejection instanceof SSLException || rejection.getClass() == SocketException.class,
              rejection.toString());
        } else {
          assertHealthEndpoint(clientWithoutCertificate, controllerUrl);
        }
      }
      assertHealthEndpoint(client, controllerUrl);
      verifyNoInteractions(admin, requestHandler, accessController, pubSubTopicRepository);
    }
  }

  @Test(timeOut = 30000)
  public void testUnreadyHealthRecordsFailureAndBalancesInflightRequests() throws Exception {
    String metricPrefix = "health_test";
    InMemoryMetricReader reader = InMemoryMetricReader.create();
    metricsRepository.close();
    metricsRepository = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(metricPrefix)
            .setMetricEntities(
                Arrays.asList(
                    SparkServerStats.SparkServerOtelMetricEntity.CALL_COUNT.getMetricEntity(),
                    SparkServerStats.SparkServerOtelMetricEntity.CALL_TIME.getMetricEntity(),
                    SparkServerStats.SparkServerOtelMetricEntity.INFLIGHT_CALL_COUNT.getMetricEntity()))
            .setEmitOtelMetrics(true)
            .setOtelAdditionalMetricsReader(reader)
            .build());
    AtomicBoolean ready = new AtomicBoolean();
    try (AdminSparkServer server = createServer(false, Optional.empty(), Collections.emptyList(), ready::get);
        CloseableHttpClient client = createClient(Optional.empty())) {
      startServer(server);
      String healthUrl = "http://localhost:" + server.getPort() + HEALTH.getPath();
      try (CloseableHttpResponse response = client.execute(new HttpGet(healthUrl))) {
        assertEquals(response.getStatusLine().getStatusCode(), HttpStatus.SC_SERVICE_UNAVAILABLE);
        assertEquals(EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8), "NOT_READY");
      }
      Collection<MetricData> metrics = reader.collectAllMetrics();
      assertEquals(
          OpenTelemetryDataTestUtils
              .getLongPointDataFromSum(
                  metrics,
                  SparkServerStats.SparkServerOtelMetricEntity.CALL_COUNT.getMetricName(),
                  metricPrefix,
                  healthAttributes(HttpStatus.SC_SERVICE_UNAVAILABLE, VeniceResponseStatusCategory.FAIL))
              .getValue(),
          1L);
      OpenTelemetryDataTestUtils.assertNoLongSumDataForAttributes(
          metrics,
          SparkServerStats.SparkServerOtelMetricEntity.CALL_COUNT.getMetricName(),
          metricPrefix,
          healthAttributes(HttpStatus.SC_SERVICE_UNAVAILABLE, VeniceResponseStatusCategory.SUCCESS));
      assertEquals(
          OpenTelemetryDataTestUtils
              .getLongPointDataFromSum(
                  metrics,
                  SparkServerStats.SparkServerOtelMetricEntity.INFLIGHT_CALL_COUNT.getMetricName(),
                  metricPrefix,
                  healthBaseAttributes())
              .getValue(),
          0L);
      ready.set(true);
      try (CloseableHttpResponse response = client.execute(new HttpGet(healthUrl))) {
        assertEquals(response.getStatusLine().getStatusCode(), HttpStatus.SC_OK);
        assertEquals(EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8), "OK");
      }
      OpenTelemetryDataTestUtils.validateLongPointDataFromCounter(
          reader,
          1,
          healthAttributes(HttpStatus.SC_OK, VeniceResponseStatusCategory.SUCCESS),
          SparkServerStats.SparkServerOtelMetricEntity.CALL_COUNT.getMetricName(),
          metricPrefix);
      assertTrue(server.isApiServing());
      server.stop();
      assertFalse(server.isApiServing());
    }
  }

  private Attributes healthBaseAttributes() {
    return Attributes.builder()
        .put(
            VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(),
            SparkServerStats.NON_CLUSTER_SPECIFIC_STAT_CLUSTER_NAME)
        .put(VENICE_CONTROLLER_ENDPOINT.getDimensionNameInDefaultFormat(), "health")
        .build();
  }

  private Attributes healthAttributes(int statusCode, VeniceResponseStatusCategory category) {
    return healthBaseAttributes().toBuilder()
        .put(HTTP_RESPONSE_STATUS_CODE.getDimensionNameInDefaultFormat(), String.valueOf(statusCode))
        .put(
            HTTP_RESPONSE_STATUS_CODE_CATEGORY.getDimensionNameInDefaultFormat(),
            HttpResponseStatusCodeCategory.getVeniceHttpResponseStatusCodeCategory(statusCode).getDimensionValue())
        .put(VENICE_RESPONSE_STATUS_CODE_CATEGORY.getDimensionNameInDefaultFormat(), category.getDimensionValue())
        .build();
  }

  private AdminSparkServer createServer(
      boolean enforceSSL,
      Optional<SSLConfig> sslConfig,
      List<ControllerRoute> disabledRoutes) {
    return createServer(enforceSSL, sslConfig, disabledRoutes, () -> true);
  }

  private AdminSparkServer createServer(
      boolean enforceSSL,
      Optional<SSLConfig> sslConfig,
      List<ControllerRoute> disabledRoutes,
      BooleanSupplier apiReadiness) {
    return new AdminSparkServer(
        TestUtils.getFreePort(),
        admin,
        metricsRepository,
        Collections.emptySet(),
        enforceSSL,
        sslConfig,
        false,
        Optional.of(accessController),
        disabledRoutes,
        VeniceProperties.empty(),
        false,
        pubSubTopicRepository,
        requestHandler,
        apiReadiness);
  }

  private void startServer(AdminSparkServer server) {
    server.start();
    clearInvocations(admin, requestHandler, accessController, pubSubTopicRepository);
  }

  private CloseableHttpClient createClient(Optional<SSLFactory> sslFactory) {
    HttpClientBuilder builder = HttpClients.custom()
        .setDefaultRequestConfig(REQUEST_CONFIG)
        .disableAutomaticRetries()
        .disableRedirectHandling();
    sslFactory.ifPresent(factory -> builder.setSSLContext(factory.getSSLContext()));
    return builder.build();
  }

  private void assertHealthEndpoint(CloseableHttpClient client, String controllerUrl) throws IOException {
    assertHealthEndpoint(client, controllerUrl, HttpStatus.SC_OK, "OK");
  }

  private void assertHealthEndpoint(
      CloseableHttpClient client,
      String controllerUrl,
      int expectedStatus,
      String expectedBody) throws IOException {
    try (CloseableHttpResponse response = client.execute(new HttpGet(controllerUrl + HEALTH.getPath()))) {
      assertEquals(response.getStatusLine().getStatusCode(), expectedStatus);
      assertEquals(ContentType.get(response.getEntity()).getMimeType(), HttpConstants.TEXT_PLAIN);
      assertEquals(EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8), expectedBody);
    }
    try (CloseableHttpResponse response = client.execute(new HttpPost(controllerUrl + HEALTH.getPath()))) {
      assertEquals(response.getStatusLine().getStatusCode(), HttpStatus.SC_NOT_FOUND);
    }
  }
}
