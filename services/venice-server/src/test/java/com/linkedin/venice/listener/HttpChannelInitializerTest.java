package com.linkedin.venice.listener;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.acl.StaticAccessController;
import com.linkedin.venice.helix.HelixCustomizedViewOfflinePushRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.security.SSLFactory;
import io.tehuti.metrics.MetricsRepository;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class HttpChannelInitializerTest {
  private ReadOnlyStoreRepository storeMetadataRepository;
  private CompletableFuture<HelixCustomizedViewOfflinePushRepository> customizedViewRepository;
  private MetricsRepository metricsRepository;
  private Optional<SSLFactory> sslFactory;
  private Optional<Executor> sslHandshakeExecutor;
  private VeniceServerConfig serverConfig;
  private Optional<StaticAccessController> accessController;
  private Optional<DynamicAccessController> storeAccessController;
  private StorageReadRequestsHandler requestHandler;

  @BeforeMethod
  public void setUp() {
    storeMetadataRepository = mock(ReadOnlyStoreRepository.class);
    metricsRepository = new MetricsRepository();
    sslFactory = Optional.of(mock(SSLFactory.class));
    sslHandshakeExecutor = Optional.of(mock(Executor.class));
    accessController = Optional.of(mock(StaticAccessController.class));
    storeAccessController = Optional.of(mock(DynamicAccessController.class));
    requestHandler = mock(StorageReadRequestsHandler.class);
    serverConfig = mock(VeniceServerConfig.class);
    customizedViewRepository = new CompletableFuture<>();
  }

  @Test
  public void testQuotaEnforcementEnabled() {
    doReturn(true).when(serverConfig).isQuotaEnforcementEnabled();
    doReturn(10l).when(serverConfig).getNodeCapacityInRcu();
    HttpChannelInitializer initializer = new HttpChannelInitializer(
        storeMetadataRepository,
        customizedViewRepository,
        metricsRepository,
        sslFactory,
        sslHandshakeExecutor,
        serverConfig,
        accessController,
        storeAccessController,
        requestHandler);
    Assert.assertNotNull(initializer.getQuotaEnforcer());
  }

  @Test
  public void testQuotaEnforcementDisabled() {
    doReturn(false).when(serverConfig).isQuotaEnforcementEnabled();
    doReturn(10l).when(serverConfig).getNodeCapacityInRcu();
    HttpChannelInitializer initializer = new HttpChannelInitializer(
        storeMetadataRepository,
        customizedViewRepository,
        metricsRepository,
        sslFactory,
        sslHandshakeExecutor,
        serverConfig,
        accessController,
        storeAccessController,
        requestHandler);
    Assert.assertNull(initializer.getQuotaEnforcer());
  }
}
