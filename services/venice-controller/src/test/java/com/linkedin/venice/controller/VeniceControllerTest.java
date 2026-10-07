package com.linkedin.venice.controller;

import static com.linkedin.venice.CommonConfigKeys.SSL_NEEDS_CLIENT_CERT;
import static com.linkedin.venice.controller.ParentControllerRegionState.ACTIVE;
import static com.linkedin.venice.controller.ParentControllerRegionState.PASSIVE;
import static com.linkedin.venice.controllerapi.ControllerRoute.HEALTH;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.SSLConfig;
import com.linkedin.venice.controller.kafka.TopicCleanupService;
import com.linkedin.venice.controller.server.AdminSparkServer;
import com.linkedin.venice.controller.systemstore.SystemStoreRepairService;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.grpc.VeniceGrpcServer;
import com.linkedin.venice.pubsub.PubSubClientsFactory;
import com.linkedin.venice.pubsub.PubSubPositionTypeRegistry;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.security.DefaultSSLFactory;
import com.linkedin.venice.servicediscovery.ServiceDiscoveryAnnouncer;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.LogContext;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.mockito.MockedConstruction;
import org.mockito.ScopedMock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class VeniceControllerTest {
  private static final String TEST_CLUSTER = "test_cluster";
  private static final int TIMEOUT_SECONDS = 10;

  @Test(timeOut = 30000)
  public void testBothListenersRemainUnreadyUntilOwnerStartupCompletes() throws Exception {
    try (OwnerFixture fixture = new OwnerFixture(false, ACTIVE, true)) {
      OwnedController owned = fixture.createController();
      BlockingCall cleanupStart = fixture.blockCleanupStart();
      BlockingCall grpcStart = fixture.blockGrpcStart(owned.controller.getAdminSecureGrpcServer());
      Future<?> startup = fixture.startAsync(owned.controller);
      cleanupStart.awaitEntered();
      assertTrue(owned.controller.getAdminServer().isApiServing());
      assertTrue(owned.controller.getSecureAdminServer().isApiServing());
      fixture.assertBothHealth(owned, HttpStatus.SC_SERVICE_UNAVAILABLE);
      expectThrows(VeniceException.class, owned.controller::start);
      fixture.assertBothHealth(owned, HttpStatus.SC_SERVICE_UNAVAILABLE);

      cleanupStart.release();
      grpcStart.awaitEntered();
      fixture.assertBothHealth(owned, HttpStatus.SC_SERVICE_UNAVAILABLE);
      grpcStart.release();
      fixture.await(startup);
      fixture.assertBothHealth(owned, HttpStatus.SC_OK);
    }
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class, timeOut = 30000)
  public void testStartupFailureKeepsBothListenersUnready(boolean failGrpcStart) throws Exception {
    try (OwnerFixture fixture = new OwnerFixture(false, ACTIVE, failGrpcStart)) {
      OwnedController owned = fixture.createController();
      VeniceException failure = new VeniceException("Required startup component failed");
      if (failGrpcStart) {
        doThrow(failure).when(owned.controller.getAdminSecureGrpcServer()).start();
      } else {
        doThrow(failure).when(fixture.cleanupService()).start();
      }
      assertSame(expectThrows(VeniceException.class, owned.controller::start), failure);
      assertTrue(owned.controller.getVeniceControllerService().isRunning());
      assertTrue(owned.controller.getAdminServer().isApiServing());
      assertTrue(owned.controller.getSecureAdminServer().isApiServing());
      fixture.assertBothHealth(owned, HttpStatus.SC_SERVICE_UNAVAILABLE);
    }
  }

  @Test(timeOut = 30000)
  public void testHttpOnlyOwnerBecomesReadyWithoutSecureListener() throws Exception {
    try (OwnerFixture fixture = new OwnerFixture(false, ACTIVE, false, false)) {
      OwnedController owned = fixture.createController();
      assertNull(owned.controller.getSecureAdminServer());
      BlockingCall cleanupStart = fixture.blockCleanupStart();
      Future<?> startup = fixture.startAsync(owned.controller);
      cleanupStart.awaitEntered();
      fixture.assertHealth(owned, false, HttpStatus.SC_SERVICE_UNAVAILABLE);
      cleanupStart.release();
      fixture.await(startup);
      fixture.assertHealth(owned, false, HttpStatus.SC_OK);
    }
  }

  @Test(timeOut = 30000)
  public void testSecureListenerStartupFailureDoesNotMakeHttpReady() throws Exception {
    try (ServerSocket occupiedPort = new ServerSocket(0);
        OwnerFixture fixture = new OwnerFixture(false, ACTIVE, false)) {
      OwnedController owned = fixture.createController(occupiedPort.getLocalPort());
      try {
        expectThrows(VeniceException.class, owned.controller::start);
        assertTrue(owned.controller.getAdminServer().isApiServing());
        assertFalse(owned.controller.getSecureAdminServer().isApiServing());
        fixture.assertHealth(owned, false, HttpStatus.SC_SERVICE_UNAVAILABLE);
      } finally {
        // A failed listener start stays STARTING, so the base service's close does not stop it.
        owned.controller.getSecureAdminServer().stopInner();
      }
    }
  }

  @Test(timeOut = 30000)
  public void testDrainingPrecedesServiceDiscoveryUnregister() throws Exception {
    try (OwnerFixture fixture = new OwnerFixture(false, ACTIVE, false)) {
      OwnedController owned = fixture.createController();
      owned.controller.start();
      fixture.assertBothHealth(owned, HttpStatus.SC_OK);
      BlockingCall unregister = fixture.blockUnregister();
      Future<?> shutdown = fixture.stopAsync(owned.controller);
      unregister.awaitEntered();
      assertTrue(owned.controller.getVeniceControllerService().isRunning());
      assertTrue(owned.controller.getAdminServer().isRunning());
      assertTrue(owned.controller.getSecureAdminServer().isRunning());
      fixture.assertBothHealth(owned, HttpStatus.SC_SERVICE_UNAVAILABLE);
      unregister.release();
      fixture.await(shutdown);
      assertFalse(owned.controller.getAdminServer().isApiServing());
      assertFalse(owned.controller.getSecureAdminServer().isApiServing());
    }
  }

  @Test(timeOut = 30000)
  public void testLateStartupCompletionCannotRestoreReadinessAfterStopBegins() throws Exception {
    try (OwnerFixture fixture = new OwnerFixture(false, ACTIVE, false)) {
      OwnedController owned = fixture.createController();
      BlockingCall cleanupStart = fixture.blockCleanupStart();
      BlockingCall unregister = fixture.blockUnregister();
      Future<?> startup = fixture.startAsync(owned.controller);
      cleanupStart.awaitEntered();
      Future<?> shutdown = fixture.stopAsync(owned.controller);
      unregister.awaitEntered();
      fixture.assertBothHealth(owned, HttpStatus.SC_SERVICE_UNAVAILABLE);

      cleanupStart.release();
      fixture.await(startup);
      assertTrue(owned.controller.getAdminServer().isApiServing());
      assertTrue(owned.controller.getSecureAdminServer().isApiServing());
      fixture.assertBothHealth(owned, HttpStatus.SC_SERVICE_UNAVAILABLE);
      unregister.release();
      fixture.await(shutdown);
    }
  }

  @DataProvider(name = "controllerRoles")
  public Object[][] controllerRoles() {
    return new Object[][] { { false, ACTIVE, true, HttpStatus.SC_OK }, { false, ACTIVE, false, HttpStatus.SC_OK },
        { false, PASSIVE, false, HttpStatus.SC_OK }, { true, ACTIVE, true, HttpStatus.SC_OK },
        { true, ACTIVE, false, HttpStatus.SC_OK }, { true, PASSIVE, true, HttpStatus.SC_SERVICE_UNAVAILABLE },
        { true, PASSIVE, false, HttpStatus.SC_SERVICE_UNAVAILABLE } };
  }

  @Test(dataProvider = "controllerRoles", timeOut = 30000)
  public void testReadinessAllowsEligibleLeadersAndStandbys(
      boolean parent,
      ParentControllerRegionState regionState,
      boolean leader,
      int expectedStatus) throws Exception {
    try (OwnerFixture fixture = new OwnerFixture(parent, regionState, false)) {
      when(fixture.admin.isLeaderControllerFor(anyString())).thenReturn(leader);
      OwnedController owned = fixture.createController();
      owned.controller.start();
      clearInvocations(fixture.admin, fixture.commonConfig, fixture.latestConfig);
      fixture.assertBothHealth(owned, expectedStatus);
      verifyNoInteractions(fixture.admin, fixture.commonConfig, fixture.latestConfig);
    }
  }

  @Test(timeOut = 30000)
  public void testStoppedCoreMakesBothListenersUnready() throws Exception {
    try (OwnerFixture fixture = new OwnerFixture(false, ACTIVE, false)) {
      OwnedController owned = fixture.createController();
      owned.controller.start();
      fixture.assertBothHealth(owned, HttpStatus.SC_OK);
      owned.controller.getVeniceControllerService().stop();
      assertTrue(owned.controller.getAdminServer().isApiServing());
      assertTrue(owned.controller.getSecureAdminServer().isApiServing());
      fixture.assertBothHealth(owned, HttpStatus.SC_SERVICE_UNAVAILABLE);
    }
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class, timeOut = 30000)
  public void testListenerDrainingGatesTheOtherListener(boolean drainSecureListener) throws Exception {
    try (OwnerFixture fixture = new OwnerFixture(false, ACTIVE, false)) {
      OwnedController owned = fixture.createController();
      owned.controller.start();
      fixture.assertBothHealth(owned, HttpStatus.SC_OK);
      AdminSparkServer drainingListener =
          drainSecureListener ? owned.controller.getSecureAdminServer() : owned.controller.getAdminServer();
      drainingListener.stopInner();
      assertTrue(drainingListener.isRunning());
      assertFalse(drainingListener.isApiServing());
      fixture.assertHealth(owned, !drainSecureListener, HttpStatus.SC_SERVICE_UNAVAILABLE);
    }
  }

  @Test(timeOut = 30000)
  public void testFreshOwnerDoesNotReuseStoppedOwnerReadiness() throws Exception {
    try (OwnerFixture fixture = new OwnerFixture(false, ACTIVE, false)) {
      OwnedController original = fixture.createController();
      original.controller.start();
      fixture.assertBothHealth(original, HttpStatus.SC_OK);
      original.controller.stop();

      OwnedController replacement = fixture.createController();
      assertFalse(replacement.controller.getAdminServer().isApiServing());
      assertFalse(replacement.controller.getSecureAdminServer().isApiServing());
      BlockingCall cleanupStart = fixture.blockCleanupStart();
      Future<?> startup = fixture.startAsync(replacement.controller);
      cleanupStart.awaitEntered();
      fixture.assertBothHealth(replacement, HttpStatus.SC_SERVICE_UNAVAILABLE);
      cleanupStart.release();
      fixture.await(startup);
      fixture.assertBothHealth(replacement, HttpStatus.SC_OK);
    }
  }

  private static final class OwnedController {
    private final VeniceController controller;
    private final int httpPort;
    private final int httpsPort;

    private OwnedController(VeniceController controller, int httpPort, int httpsPort) {
      this.controller = controller;
      this.httpPort = httpPort;
      this.httpsPort = httpsPort;
    }
  }

  private static final class BlockingCall implements Answer<Void> {
    private final CountDownLatch entered = new CountDownLatch(1);
    private final CountDownLatch released = new CountDownLatch(1);

    @Override
    public Void answer(InvocationOnMock invocation) throws InterruptedException {
      entered.countDown();
      assertTrue(released.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Lifecycle test barrier was not released");
      return null;
    }

    private void awaitEntered() throws InterruptedException {
      assertTrue(entered.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Lifecycle test barrier was not reached");
    }

    private void release() {
      released.countDown();
    }
  }

  private static final class OwnerFixture implements AutoCloseable {
    private final boolean parent;
    private final boolean grpcEnabled;
    private final boolean sslEnabled;
    private final Admin admin;
    private final VeniceControllerClusterConfig commonConfig = mock(VeniceControllerClusterConfig.class);
    private final ServiceDiscoveryAnnouncer announcer = mock(ServiceDiscoveryAnnouncer.class);
    private final List<ScopedMock> constructionMocks = new ArrayList<>();
    private final List<OwnedController> controllers = new ArrayList<>();
    private final List<MetricsRepository> metricsRepositories = new ArrayList<>();
    private final List<BlockingCall> barriers = new ArrayList<>();
    private final List<Future<?>> pendingCalls = new ArrayList<>();
    private final ExecutorService executor = Executors.newFixedThreadPool(2);
    private final MockedConstruction<TopicCleanupService> cleanupServices;
    private final SSLConfig sslConfig;
    private final CloseableHttpClient client;
    private VeniceControllerMultiClusterConfig latestConfig;
    private int nextHttpPort;
    private int nextHttpsPort;

    private OwnerFixture(boolean parent, ParentControllerRegionState regionState, boolean grpcEnabled) {
      this(parent, regionState, grpcEnabled, true);
    }

    private OwnerFixture(
        boolean parent,
        ParentControllerRegionState regionState,
        boolean grpcEnabled,
        boolean sslEnabled) {
      this.parent = parent;
      this.grpcEnabled = grpcEnabled;
      this.sslEnabled = sslEnabled;
      this.admin = parent ? mock(VeniceParentHelixAdmin.class) : mock(VeniceHelixAdmin.class);
      when(admin.getLogContext()).thenReturn(LogContext.EMPTY);
      when(commonConfig.isParent()).thenReturn(parent);
      when(commonConfig.getParentControllerRegionState()).thenReturn(regionState);
      when(commonConfig.getJettyConfigOverrides()).thenReturn(VeniceProperties.empty());

      Properties sslProperties = SslUtils.getVeniceLocalSslProperties();
      sslProperties.setProperty(SSL_NEEDS_CLIENT_CERT, "true");
      sslConfig = new SSLConfig(new VeniceProperties(sslProperties));
      client = HttpClients.custom()
          .setSSLContext(SslUtils.getVeniceLocalSslFactory().getSSLContext())
          .setDefaultRequestConfig(
              RequestConfig.custom()
                  .setConnectTimeout(5000)
                  .setConnectionRequestTimeout(5000)
                  .setSocketTimeout(5000)
                  .build())
          .disableAutomaticRetries()
          .disableRedirectHandling()
          .build();

      track(mockConstruction(VeniceControllerMultiClusterConfig.class, this::initializeConfig));
      track(mockConstruction(VeniceControllerService.class, (service, context) -> {
        AtomicBoolean running = new AtomicBoolean();
        when(service.getVeniceHelixAdmin()).thenReturn(admin);
        when(service.isRunning()).thenAnswer(invocation -> running.get());
        doAnswer(invocation -> {
          running.set(true);
          return null;
        }).when(service).start();
        doAnswer(invocation -> {
          running.set(false);
          return null;
        }).when(service).stop();
        doAnswer(invocation -> {
          running.set(false);
          return null;
        }).when(service).close();
      }));
      cleanupServices = track(mockConstruction(TopicCleanupService.class));
      track(mockConstruction(StoreBackupVersionCleanupService.class));
      track(mockConstruction(DisabledPartitionEnablerService.class));
      track(mockConstruction(UnusedValueSchemaCleanupService.class));
      track(mockConstruction(StoreGraveyardCleanupService.class));
      track(mockConstruction(SystemStoreRepairService.class));
      track(mockConstruction(DeferredVersionSwapService.class));
      track(mockConstruction(VeniceGrpcServer.class));
    }

    private void initializeConfig(VeniceControllerMultiClusterConfig config, MockedConstruction.Context context) {
      latestConfig = config;
      when(config.getCommonConfig()).thenReturn(commonConfig);
      when(config.getLogContext()).thenReturn(LogContext.EMPTY);
      when(config.isParent()).thenReturn(parent);
      when(config.getClusters()).thenReturn(Collections.singleton(TEST_CLUSTER));
      when(config.getAdminPort()).thenReturn(nextHttpPort);
      when(config.getAdminSecurePort()).thenReturn(nextHttpsPort);
      when(config.getAdminGrpcPort()).thenReturn(TestUtils.getFreePort());
      when(config.getAdminSecureGrpcPort()).thenReturn(TestUtils.getFreePort());
      when(config.getSslConfig()).thenReturn(sslEnabled ? Optional.of(sslConfig) : Optional.empty());
      when(config.getDisabledRoutes()).thenReturn(Collections.emptyList());
      when(config.isControllerEnforceSSLOnly()).thenReturn(true);
      when(config.getServiceDiscoveryRegistrationRetryMS()).thenReturn(100L);
      when(config.getPubSubTopicRepository()).thenReturn(new PubSubTopicRepository());
      when(config.getPubSubClientsFactory()).thenReturn(mock(PubSubClientsFactory.class));
      when(config.getPubSubPositionTypeRegistry()).thenReturn(mock(PubSubPositionTypeRegistry.class));
      when(config.getSystemSchemaClusterName()).thenReturn(TEST_CLUSTER);
      when(config.getControllerConfig(TEST_CLUSTER)).thenReturn(commonConfig);
      when(config.isGrpcServerEnabled()).thenReturn(grpcEnabled);
      when(config.getGrpcServerThreadCount()).thenReturn(1);
      when(config.getSslFactoryClassName()).thenReturn(DefaultSSLFactory.class.getName());
    }

    private <T> MockedConstruction<T> track(MockedConstruction<T> constructionMock) {
      constructionMocks.add(constructionMock);
      return constructionMock;
    }

    private OwnedController createController() {
      return createController(TestUtils.getFreePort());
    }

    private OwnedController createController(int httpsPort) {
      nextHttpPort = TestUtils.getFreePort();
      nextHttpsPort = httpsPort;
      MetricsRepository metricsRepository = new MetricsRepository();
      metricsRepositories.add(metricsRepository);
      VeniceController controller = new VeniceController(
          new VeniceControllerContext.Builder().setPropertiesList(Collections.emptyList())
              .setMetricsRepository(metricsRepository)
              .setServiceDiscoveryAnnouncers(Collections.singletonList(announcer))
              .build());
      OwnedController owned = new OwnedController(controller, nextHttpPort, nextHttpsPort);
      controllers.add(owned);
      return owned;
    }

    private TopicCleanupService cleanupService() {
      List<TopicCleanupService> constructed = cleanupServices.constructed();
      return constructed.get(constructed.size() - 1);
    }

    private BlockingCall blockCleanupStart() {
      BlockingCall barrier = new BlockingCall();
      barriers.add(barrier);
      doAnswer(barrier).when(cleanupService()).start();
      return barrier;
    }

    private BlockingCall blockGrpcStart(VeniceGrpcServer server) {
      BlockingCall barrier = new BlockingCall();
      barriers.add(barrier);
      doAnswer(barrier).when(server).start();
      return barrier;
    }

    private BlockingCall blockUnregister() {
      BlockingCall barrier = new BlockingCall();
      barriers.add(barrier);
      doAnswer(barrier).when(announcer).unregister();
      return barrier;
    }

    private Future<?> startAsync(VeniceController controller) {
      Future<?> future = executor.submit(controller::start);
      pendingCalls.add(future);
      return future;
    }

    private Future<?> stopAsync(VeniceController controller) {
      Future<?> future = executor.submit(controller::stop);
      pendingCalls.add(future);
      return future;
    }

    private void await(Future<?> future) throws Exception {
      future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
      pendingCalls.remove(future);
    }

    private void assertBothHealth(OwnedController owned, int expectedStatus) throws IOException {
      assertHealth(owned, false, expectedStatus);
      assertHealth(owned, true, expectedStatus);
    }

    private void assertHealth(OwnedController owned, boolean secure, int expectedStatus) throws IOException {
      String url = (secure ? "https" : "http") + "://localhost:" + (secure ? owned.httpsPort : owned.httpPort)
          + HEALTH.getPath();
      try (CloseableHttpResponse response = client.execute(new HttpGet(url))) {
        assertEquals(response.getStatusLine().getStatusCode(), expectedStatus);
        assertEquals(ContentType.get(response.getEntity()).getMimeType(), HttpConstants.TEXT_PLAIN);
        assertEquals(
            EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8),
            expectedStatus == HttpStatus.SC_OK ? "OK" : "NOT_READY");
      }
    }

    @Override
    public void close() throws Exception {
      barriers.forEach(BlockingCall::release);
      try {
        for (Future<?> future: pendingCalls) {
          future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
      } finally {
        try {
          for (OwnedController owned: controllers) {
            owned.controller.stop();
          }
        } finally {
          try {
            client.close();
            metricsRepositories.forEach(MetricsRepository::close);
          } finally {
            executor.shutdownNow();
            try {
              assertTrue(
                  executor.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS),
                  "Lifecycle executor did not stop");
            } finally {
              for (int i = constructionMocks.size() - 1; i >= 0; i--) {
                constructionMocks.get(i).close();
              }
            }
          }
        }
      }
    }
  }
}
