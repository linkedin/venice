package com.linkedin.venice.controller.server;

import static com.linkedin.venice.CommonConfigKeys.SSL_NEEDS_CLIENT_CERT;
import static com.linkedin.venice.controller.ParentControllerRegionState.ACTIVE;
import static com.linkedin.venice.controller.ParentControllerRegionState.PASSIVE;
import static com.linkedin.venice.controllerapi.ControllerRoute.HEALTH;
import static com.linkedin.venice.controllerapi.ControllerRoute.NEW_STORE;
import static com.linkedin.venice.controllerapi.ControllerRoute.STORE;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.SSLConfig;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.ParentControllerRegionState;
import com.linkedin.venice.controllerapi.ControllerRoute;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.LogContext;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;
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
import org.testng.annotations.DataProvider;
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

  @DataProvider(name = "controllerStates")
  public Object[][] controllerStates() {
    return new Object[][] { { false, false, ACTIVE }, { false, true, ACTIVE }, { false, true, PASSIVE },
        { true, false, ACTIVE }, { true, true, ACTIVE }, { true, true, PASSIVE } };
  }

  @Test(dataProvider = "controllerStates", timeOut = 30000)
  public void testHttpHealthDoesNotRequireControllerReadiness(
      boolean enforceSSL,
      boolean isParent,
      ParentControllerRegionState regionState) throws IOException {
    when(admin.isParent()).thenReturn(isParent);
    when(admin.getParentControllerRegionState()).thenReturn(regionState);
    when(admin.isLeaderControllerFor(anyString())).thenReturn(false);

    try (AdminSparkServer server = createServer(enforceSSL, Optional.empty(), Collections.emptyList());
        CloseableHttpClient client = createClient(Optional.empty())) {
      startServer(server);
      assertHealthEndpoint(client, "http://localhost:" + server.getPort());
      verifyNoInteractions(admin, requestHandler, accessController, pubSubTopicRepository);
    }
  }

  @DataProvider(name = "sslEnabled")
  public Object[][] sslEnabled() {
    return new Object[][] { { false }, { true } };
  }

  @Test(dataProvider = "sslEnabled", timeOut = 30000)
  public void testHealthHonorsDisabledRoutes(boolean sslEnabled) throws IOException {
    Optional<SSLConfig> sslConfig = sslEnabled
        ? Optional.of(new SSLConfig(new VeniceProperties(SslUtils.getVeniceLocalSslProperties())))
        : Optional.empty();
    Optional<SSLFactory> sslFactory = sslEnabled ? Optional.of(SslUtils.getVeniceLocalSslFactory()) : Optional.empty();

    try (AdminSparkServer server = createServer(true, sslConfig, Collections.singletonList(HEALTH));
        CloseableHttpClient client = createClient(sslFactory)) {
      startServer(server);
      String healthUrl = (sslEnabled ? "https" : "http") + "://localhost:" + server.getPort() + HEALTH.getPath();
      try (CloseableHttpResponse response = client.execute(new HttpGet(healthUrl))) {
        assertEquals(response.getStatusLine().getStatusCode(), HttpStatus.SC_FORBIDDEN);
        assertEquals(
            EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8),
            "Route /health has been disabled in venice controller config!!");
      }
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

  @Test(dataProvider = "sslEnabled", timeOut = 30000)
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
          expectThrows(SSLHandshakeException.class, () -> {
            try (CloseableHttpResponse response =
                clientWithoutCertificate.execute(new HttpGet(controllerUrl + HEALTH.getPath()))) {
              assertEquals(response.getStatusLine().getStatusCode(), HttpStatus.SC_OK);
            }
          });
        } else {
          assertHealthEndpoint(clientWithoutCertificate, controllerUrl);
        }
      }
      verifyNoInteractions(admin, requestHandler, accessController, pubSubTopicRepository);
    }
  }

  private AdminSparkServer createServer(
      boolean enforceSSL,
      Optional<SSLConfig> sslConfig,
      List<ControllerRoute> disabledRoutes) {
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
        requestHandler);
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
    try (CloseableHttpResponse response = client.execute(new HttpGet(controllerUrl + HEALTH.getPath()))) {
      assertEquals(response.getStatusLine().getStatusCode(), HttpStatus.SC_OK);
      assertEquals(ContentType.get(response.getEntity()).getMimeType(), HttpConstants.TEXT_PLAIN);
      assertEquals(EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8), "OK");
    }
    try (CloseableHttpResponse response = client.execute(new HttpPost(controllerUrl + HEALTH.getPath()))) {
      assertEquals(response.getStatusLine().getStatusCode(), HttpStatus.SC_NOT_FOUND);
    }
  }
}
