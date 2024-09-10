package com.linkedin.venice.listener;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.acl.StaticAccessController;
import com.linkedin.venice.authorization.DefaultIdentityParser;
import com.linkedin.venice.helix.HelixCustomizedViewOfflinePushRepository;
import com.linkedin.venice.listener.grpc.handlers.VeniceServerGrpcRequestProcessor;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.security.SSLConfig;
import com.linkedin.venice.security.SSLFactory;
import io.grpc.ServerInterceptor;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.tehuti.metrics.MetricsRepository;
import java.util.List;
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
  private Optional<SSLFactory> sslFactoryOptional;

  private SSLFactory sslFactory;
  private Executor sslHandshakeExecutor;
  private VeniceServerConfig serverConfig;
  private Optional<StaticAccessController> accessController;
  private Optional<DynamicAccessController> storeAccessController;
  private StorageReadRequestHandler requestHandler;

  @BeforeMethod
  public void setUp() {
    storeMetadataRepository = mock(ReadOnlyStoreRepository.class);
    metricsRepository = new MetricsRepository();
    sslFactory = mock(SSLFactory.class);
    sslFactoryOptional = Optional.of(sslFactory);
    sslHandshakeExecutor = mock(Executor.class);
    accessController = Optional.of(mock(StaticAccessController.class));
    storeAccessController = Optional.of(mock(DynamicAccessController.class));
    requestHandler = mock(StorageReadRequestHandler.class);
    serverConfig = mock(VeniceServerConfig.class);
    customizedViewRepository = new CompletableFuture<>();
    doReturn(DefaultIdentityParser.class.getName()).when(serverConfig).getIdentityParserClassName();
  }

  @Test
  public void testQuotaEnforcementEnabled() {
    doReturn(true).when(serverConfig).isQuotaEnforcementEnabled();
    doReturn(10l).when(serverConfig).getNodeCapacityInRcu();
    doReturn(10000).when(serverConfig).getQuotaEnforcementIntervalInMs();
    doReturn(5).when(serverConfig).getQuotaEnforcementCapacityMultiple();
    HttpChannelInitializer initializer = new HttpChannelInitializer(
        storeMetadataRepository,
        customizedViewRepository,
        metricsRepository,
        sslFactoryOptional,
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
        sslFactoryOptional,
        sslHandshakeExecutor,
        serverConfig,
        accessController,
        storeAccessController,
        requestHandler);
    Assert.assertNull(initializer.getQuotaEnforcer());
  }

  @Test
  public void testInitChannelWithSSLExecutor() {
    SSLConfig sslConfig = new SSLConfig();
    doReturn(sslConfig).when(sslFactory).getSSLConfig();
    ChannelPipeline channelPipeline = mock(ChannelPipeline.class);
    SocketChannel ch = mock(SocketChannel.class);
    doReturn(channelPipeline).when(ch).pipeline();
    doReturn(channelPipeline).when(channelPipeline).addLast(any());
    HttpChannelInitializer initializer = new HttpChannelInitializer(
        storeMetadataRepository,
        customizedViewRepository,
        metricsRepository,
        sslFactoryOptional,
        sslHandshakeExecutor,
        serverConfig,
        accessController,
        storeAccessController,
        requestHandler);
    initializer.initChannel(ch);
  }

  @Test
  public void initGrpcRequestProcessor() {
    SSLConfig sslConfig = new SSLConfig();
    doReturn(sslConfig).when(sslFactory).getSSLConfig();
    HttpChannelInitializer initializer = new HttpChannelInitializer(
        storeMetadataRepository,
        customizedViewRepository,
        metricsRepository,
        sslFactoryOptional,
        sslHandshakeExecutor,
        serverConfig,
        accessController,
        storeAccessController,
        requestHandler);

    VeniceServerGrpcRequestProcessor processor = initializer.initGrpcRequestProcessor();
    Assert.assertNotNull(processor);
  }

  @Test
  public void initGprcHandlersTestInterceptors() {
    SSLConfig sslConfig = new SSLConfig();
    doReturn(sslConfig).when(sslFactory).getSSLConfig();
    HttpChannelInitializer initializer = new HttpChannelInitializer(
        storeMetadataRepository,
        customizedViewRepository,
        metricsRepository,
        sslFactoryOptional,
        sslHandshakeExecutor,
        serverConfig,
        accessController,
        storeAccessController,
        requestHandler);

    List<ServerInterceptor> interceptors = initializer.initGrpcInterceptors();
    Assert.assertNotNull(interceptors);
    Assert.assertEquals(interceptors.size(), 3);
    Assert.assertTrue(interceptors.get(0) instanceof VerifySslHandler);
    Assert.assertTrue(interceptors.get(1) instanceof ServerAclHandler);
    Assert.assertTrue(interceptors.get(2) instanceof ServerStoreAclHandler);
  }
}
