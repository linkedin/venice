package com.linkedin.venice.listener;

import com.linkedin.security.ssl.access.control.SSLEngineComponentFactory;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.acl.StaticAccessController;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.RoutingDataRepository;
import io.tehuti.metrics.MetricsRepository;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class HttpChannelInitializerTest {
  private ReadOnlyStoreRepository storeMetadataRepository;
  private CompletableFuture<RoutingDataRepository> routingRepository;
  private MetricsRepository metricsRepository;
  private Optional<SSLEngineComponentFactory> sslFactory;
  private VeniceServerConfig serverConfig;
  private Optional<StaticAccessController> accessController;
  private Optional<DynamicAccessController> storeAccessController;
  private StorageReadRequestsHandler requestHandler;

  @BeforeMethod
  public void setUp(){
    storeMetadataRepository = mock(ReadOnlyStoreRepository.class);
    metricsRepository = new MetricsRepository();
    sslFactory = Optional.of(mock(SSLEngineComponentFactory.class));
    accessController = Optional.of(mock(StaticAccessController.class));
    storeAccessController = Optional.of(mock(DynamicAccessController.class));
    requestHandler = mock(StorageReadRequestsHandler.class);
    serverConfig = mock(VeniceServerConfig.class);
    routingRepository = new CompletableFuture<>();
  }

  @Test
  public void testQuotaEnforcementEnabled() {
    doReturn(true).when(serverConfig).isQuotaEnforcementEnabled();
    doReturn(10l).when(serverConfig).getNodeCapacityInRcu();
    HttpChannelInitializer initializer = new HttpChannelInitializer(storeMetadataRepository,
        routingRepository,
        metricsRepository,
        sslFactory,  serverConfig,
        accessController,
        storeAccessController,
        requestHandler);
    Assert.assertNotNull(initializer.getQuotaEnforcer());
    }

  @Test
  public void testQuotaEnforcementDisabled() {
    doReturn(false).when(serverConfig).isQuotaEnforcementEnabled();
    doReturn(10l).when(serverConfig).getNodeCapacityInRcu();
    HttpChannelInitializer initializer = new HttpChannelInitializer(storeMetadataRepository,
        routingRepository,
        metricsRepository,
        sslFactory,  serverConfig,
        accessController,
        storeAccessController,
        requestHandler);
    Assert.assertNull(initializer.getQuotaEnforcer());
  }
}

