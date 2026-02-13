package com.linkedin.venice.router.httpclient;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.venice.meta.LiveInstanceMonitor;
import com.linkedin.venice.router.VeniceRouterConfig;
import com.linkedin.venice.security.SSLFactory;
import io.tehuti.metrics.MetricsRepository;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import javax.net.ssl.SSLContext;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ApacheHttpAsyncStorageNodeClientTest {
  private VeniceRouterConfig routerConfig;
  private SSLFactory sslFactory;
  private MetricsRepository metricsRepository;
  private LiveInstanceMonitor liveInstanceMonitor;

  @BeforeMethod
  public void setUp() throws Exception {
    routerConfig = mock(VeniceRouterConfig.class);
    sslFactory = mock(SSLFactory.class);
    metricsRepository = new MetricsRepository();
    liveInstanceMonitor = mock(LiveInstanceMonitor.class);

    // Default config for pool mode
    setupDefaultConfig();

    doReturn(SSLContext.getDefault()).when(sslFactory).getSSLContext();
    doReturn(Collections.emptySet()).when(liveInstanceMonitor).getAllLiveInstances();
  }

  @AfterMethod
  public void tearDown() {
    if (metricsRepository != null) {
      metricsRepository.close();
    }
  }

  private void setupDefaultConfig() {
    doReturn(4).when(routerConfig).getIoThreadCountInPoolMode();
    doReturn(100).when(routerConfig).getMaxOutgoingConnPerRoute();
    doReturn(1000).when(routerConfig).getMaxOutgoingConn();
    doReturn(false).when(routerConfig).isPerNodeClientAllocationEnabled();
    doReturn(30000).when(routerConfig).getSocketTimeout();
    doReturn(5000).when(routerConfig).getConnectionTimeout();
    doReturn(2).when(routerConfig).getHttpClientPoolSize();
    doReturn(false).when(routerConfig).isDnsCacheEnabled();
    doReturn(false).when(routerConfig).isHttpasyncclientConnectionWarmingEnabled();
  }

  @Test
  public void testConstructorInPoolMode() {
    ApacheHttpAsyncStorageNodeClient client = null;
    try {
      client = new ApacheHttpAsyncStorageNodeClient(
          routerConfig,
          Optional.of(sslFactory),
          metricsRepository,
          liveInstanceMonitor);

      Assert.assertNotNull(client);
    } finally {
      if (client != null) {
        client.close();
      }
    }
  }

  @Test
  public void testConstructorInPerNodeMode() {
    doReturn(true).when(routerConfig).isPerNodeClientAllocationEnabled();

    ApacheHttpAsyncStorageNodeClient client = null;
    try {
      client = new ApacheHttpAsyncStorageNodeClient(
          routerConfig,
          Optional.of(sslFactory),
          metricsRepository,
          liveInstanceMonitor);

      Assert.assertNotNull(client);
      // In per-node mode, clients are created in start(), not constructor
    } finally {
      if (client != null) {
        client.close();
      }
    }
  }

  @Test
  public void testGetHttpClientForHostInPoolMode() {
    ApacheHttpAsyncStorageNodeClient client = null;
    try {
      client = new ApacheHttpAsyncStorageNodeClient(
          routerConfig,
          Optional.of(sslFactory),
          metricsRepository,
          liveInstanceMonitor);

      // In pool mode, should return a client from the pool
      CloseableHttpAsyncClient httpClient = client.getHttpClientForHost("any-host");
      Assert.assertNotNull(httpClient, "Pool mode should return a client for any host");
    } finally {
      if (client != null) {
        client.close();
      }
    }
  }

  @Test
  public void testGetHttpClientForHostInPerNodeModeWithoutStart() {
    doReturn(true).when(routerConfig).isPerNodeClientAllocationEnabled();

    ApacheHttpAsyncStorageNodeClient client = null;
    try {
      client = new ApacheHttpAsyncStorageNodeClient(
          routerConfig,
          Optional.of(sslFactory),
          metricsRepository,
          liveInstanceMonitor);

      // In per-node mode without start(), no clients are created
      CloseableHttpAsyncClient httpClient = client.getHttpClientForHost("any-host");
      Assert.assertNull(httpClient, "Per-node mode without start() should return null for unknown host");
    } finally {
      if (client != null) {
        client.close();
      }
    }
  }

  @Test
  public void testIsInstanceReadyToServeWithoutConnectionWarming() {
    ApacheHttpAsyncStorageNodeClient client = null;
    try {
      client = new ApacheHttpAsyncStorageNodeClient(
          routerConfig,
          Optional.of(sslFactory),
          metricsRepository,
          liveInstanceMonitor);

      // Without connection warming, all instances should be ready to serve
      Assert.assertTrue(client.isInstanceReadyToServe("any-instance"));
    } finally {
      if (client != null) {
        client.close();
      }
    }
  }

  @Test
  public void testCloseInPoolMode() {
    ApacheHttpAsyncStorageNodeClient client = new ApacheHttpAsyncStorageNodeClient(
        routerConfig,
        Optional.of(sslFactory),
        metricsRepository,
        liveInstanceMonitor);

    // close() should not throw
    client.close();

    // Calling close again should also not throw
    client.close();
  }

  @Test
  public void testCloseInPerNodeMode() {
    doReturn(true).when(routerConfig).isPerNodeClientAllocationEnabled();

    ApacheHttpAsyncStorageNodeClient client = new ApacheHttpAsyncStorageNodeClient(
        routerConfig,
        Optional.of(sslFactory),
        metricsRepository,
        liveInstanceMonitor);

    // close() in per-node mode should not throw even without clients
    client.close();
  }

  @Test
  public void testStartInPoolMode() {
    ApacheHttpAsyncStorageNodeClient client = null;
    try {
      client = new ApacheHttpAsyncStorageNodeClient(
          routerConfig,
          Optional.of(sslFactory),
          metricsRepository,
          liveInstanceMonitor);

      // start() in pool mode is a no-op since clients are created in constructor
      client.start();
    } finally {
      if (client != null) {
        client.close();
      }
    }
  }

  @Test
  public void testStartInPerNodeModeWithNoInstances() {
    doReturn(true).when(routerConfig).isPerNodeClientAllocationEnabled();
    doReturn(1).when(routerConfig).getPerNodeClientThreadCount();
    doReturn(Collections.emptySet()).when(liveInstanceMonitor).getAllLiveInstances();

    ApacheHttpAsyncStorageNodeClient client = null;
    try {
      client = new ApacheHttpAsyncStorageNodeClient(
          routerConfig,
          Optional.of(sslFactory),
          metricsRepository,
          liveInstanceMonitor);

      // start() with no instances should not throw
      client.start();
    } finally {
      if (client != null) {
        client.close();
      }
    }
  }

  @Test
  public void testConstructorWithDnsCache() {
    doReturn(true).when(routerConfig).isDnsCacheEnabled();
    doReturn(".*").when(routerConfig).getHostPatternForDnsCache();
    doReturn(60000L).when(routerConfig).getDnsCacheRefreshIntervalInMs();

    ApacheHttpAsyncStorageNodeClient client = null;
    try {
      client = new ApacheHttpAsyncStorageNodeClient(
          routerConfig,
          Optional.of(sslFactory),
          metricsRepository,
          liveInstanceMonitor);

      Assert.assertNotNull(client);
    } finally {
      if (client != null) {
        client.close();
      }
    }
  }

  @Test
  public void testConstructorWithDifferentPoolSizes() {
    // Test with pool size of 1
    doReturn(1).when(routerConfig).getHttpClientPoolSize();
    doReturn(2).when(routerConfig).getIoThreadCountInPoolMode();

    ApacheHttpAsyncStorageNodeClient client = null;
    try {
      client = new ApacheHttpAsyncStorageNodeClient(
          routerConfig,
          Optional.of(sslFactory),
          metricsRepository,
          liveInstanceMonitor);

      Assert.assertNotNull(client);
    } finally {
      if (client != null) {
        client.close();
      }
    }
  }

  @Test
  public void testGetHttpClientForHostReturnsDifferentClients() {
    int poolSize = 4;
    doReturn(poolSize).when(routerConfig).getHttpClientPoolSize();

    ApacheHttpAsyncStorageNodeClient client = null;
    try {
      client = new ApacheHttpAsyncStorageNodeClient(
          routerConfig,
          Optional.of(sslFactory),
          metricsRepository,
          liveInstanceMonitor);

      // Make multiple requests - in pool mode, may return different clients due to random selection
      Set<CloseableHttpAsyncClient> clients = new HashSet<>();
      for (int i = 0; i < 100; i++) {
        CloseableHttpAsyncClient httpClient = client.getHttpClientForHost("host" + i);
        if (httpClient != null) {
          clients.add(httpClient);
        }
      }

      // With pool size 4 and 100 iterations, we should see all clients from the pool
      // The probability of NOT hitting all 4 clients in 100 random selections is negligible
      Assert.assertEquals(
          clients.size(),
          poolSize,
          "Pool mode should use all " + poolSize + " clients from the pool over 100 iterations");
    } finally {
      if (client != null) {
        client.close();
      }
    }
  }

  @Test
  public void testConstructorWithoutSsl() {
    ApacheHttpAsyncStorageNodeClient client = null;
    try {
      client =
          new ApacheHttpAsyncStorageNodeClient(routerConfig, Optional.empty(), metricsRepository, liveInstanceMonitor);

      Assert.assertNotNull(client);
    } finally {
      if (client != null) {
        client.close();
      }
    }
  }
}
