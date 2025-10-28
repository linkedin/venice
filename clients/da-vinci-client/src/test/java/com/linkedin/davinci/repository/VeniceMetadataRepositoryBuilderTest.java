package com.linkedin.davinci.repository;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.davinci.config.VeniceClusterConfig;
import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.helix.ZkClientFactory;
import com.linkedin.venice.service.ICProvider;
import com.linkedin.venice.stats.ZkClientStatusStats;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.mockito.MockedStatic;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class VeniceMetadataRepositoryBuilderTest {
  private VeniceConfigLoader mockConfigLoader;
  private ClientConfig mockClientConfig;
  private MetricsRepository mockMetricsRepository;
  private ICProvider mockIcProvider;
  private VeniceServerConfig mockServerConfig;
  private VeniceClusterConfig mockClusterConfig;
  private VeniceProperties mockVeniceProperties;
  private ZkClient mockZkClient;
  private NativeMetadataRepository mockNativeMetadataRepository;

  private MockedStatic<ZkClientFactory> zkClientFactoryMock;
  private MockedStatic<NativeMetadataRepository> nativeMetadataRepositoryMock;

  @BeforeMethod
  public void setUp() {
    mockConfigLoader = mock(VeniceConfigLoader.class);
    mockClientConfig = mock(ClientConfig.class);
    mockMetricsRepository = mock(MetricsRepository.class);
    mockIcProvider = mock(ICProvider.class);
    mockServerConfig = mock(VeniceServerConfig.class);
    mockClusterConfig = mock(VeniceClusterConfig.class);
    mockVeniceProperties = mock(VeniceProperties.class);
    mockZkClient = mock(ZkClient.class);
    mockNativeMetadataRepository = mock(NativeMetadataRepository.class);

    // Mock MetricsRepository to return valid sensors to avoid NullPointerException
    Sensor mockSensor = mock(Sensor.class);
    when(mockMetricsRepository.sensor(anyString(), any())).thenReturn(mockSensor);
    when(mockMetricsRepository.sensor(anyString())).thenReturn(mockSensor);

    when(mockConfigLoader.getVeniceServerConfig()).thenReturn(mockServerConfig);
    when(mockConfigLoader.getVeniceClusterConfig()).thenReturn(mockClusterConfig);
    when(mockConfigLoader.getCombinedProperties()).thenReturn(mockVeniceProperties);

    zkClientFactoryMock = mockStatic(ZkClientFactory.class);
    nativeMetadataRepositoryMock = mockStatic(NativeMetadataRepository.class);
  }

  @AfterMethod
  public void tearDown() {
    if (zkClientFactoryMock != null) {
      zkClientFactoryMock.close();
    }
    if (nativeMetadataRepositoryMock != null) {
      nativeMetadataRepositoryMock.close();
    }
  }

  @Test
  public void testConstructorWithDaVinciClientAndNoIngestionIsolation() {
    // Setup
    when(mockServerConfig.isDaVinciClient()).thenReturn(true);
    nativeMetadataRepositoryMock.when(() -> NativeMetadataRepository.getInstance(any(), any(), any()))
        .thenReturn(mockNativeMetadataRepository);

    // Execute
    VeniceMetadataRepositoryBuilder builder = new VeniceMetadataRepositoryBuilder(
        mockConfigLoader,
        mockClientConfig,
        mockMetricsRepository,
        mockIcProvider,
        false // isIngestionIsolation
    );

    // Verify
    verify(mockClientConfig).setMetricsRepository(mockMetricsRepository);
    verify(mockNativeMetadataRepository).start();
    verify(mockNativeMetadataRepository).refresh();

    Assert.assertTrue(builder.isDaVinciClient());
    Assert.assertNotNull(builder.getStoreRepo());
    Assert.assertNotNull(builder.getSchemaRepo());
    Assert.assertNotNull(builder.getClusterInfoProvider());
    Assert.assertNull(builder.getLiveClusterConfigRepo());
    Assert.assertNull(builder.getZkClient());
    Assert.assertFalse(builder.getReadOnlyZKSharedSchemaRepository().isPresent());
  }

  @Test
  public void testConstructorWithDaVinciClientAndIngestionIsolation() {
    // Setup
    when(mockServerConfig.isDaVinciClient()).thenReturn(true);
    when(mockClusterConfig.getZookeeperAddress()).thenReturn("localhost:2181");
    when(mockClusterConfig.getClusterName()).thenReturn("test-cluster");
    when(mockServerConfig.getSystemSchemaClusterName()).thenReturn("system-cluster");
    when(mockClusterConfig.getRefreshAttemptsForZkReconnect()).thenReturn(3);
    when(mockClusterConfig.getRefreshIntervalForZkReconnectInMs()).thenReturn(1000L);

    zkClientFactoryMock.when(() -> ZkClientFactory.newZkClient(anyString())).thenReturn(mockZkClient);

    // Execute
    VeniceMetadataRepositoryBuilder builder = new VeniceMetadataRepositoryBuilder(
        mockConfigLoader,
        mockClientConfig,
        mockMetricsRepository,
        mockIcProvider,
        true // isIngestionIsolation
    );

    // Verify
    verify(mockClientConfig).setMetricsRepository(mockMetricsRepository);
    zkClientFactoryMock.verify(() -> ZkClientFactory.newZkClient("localhost:2181"));
    verify(mockZkClient).subscribeStateChanges(any(ZkClientStatusStats.class));

    Assert.assertTrue(builder.isDaVinciClient());
    Assert.assertNotNull(builder.getStoreRepo());
    Assert.assertNotNull(builder.getSchemaRepo());
    Assert.assertNotNull(builder.getClusterInfoProvider());
    Assert.assertNotNull(builder.getLiveClusterConfigRepo());
    Assert.assertNotNull(builder.getZkClient());
    Assert.assertTrue(builder.getReadOnlyZKSharedSchemaRepository().isPresent());
  }

  @Test
  public void testConstructorWithServerAndNoIngestionIsolation() {
    // Setup
    when(mockServerConfig.isDaVinciClient()).thenReturn(false);
    when(mockClusterConfig.getZookeeperAddress()).thenReturn("localhost:2181");
    when(mockClusterConfig.getClusterName()).thenReturn("test-cluster");
    when(mockServerConfig.getSystemSchemaClusterName()).thenReturn("system-cluster");
    when(mockClusterConfig.getRefreshAttemptsForZkReconnect()).thenReturn(3);
    when(mockClusterConfig.getRefreshIntervalForZkReconnectInMs()).thenReturn(1000L);

    zkClientFactoryMock.when(() -> ZkClientFactory.newZkClient(anyString())).thenReturn(mockZkClient);

    // Execute
    VeniceMetadataRepositoryBuilder builder = new VeniceMetadataRepositoryBuilder(
        mockConfigLoader,
        mockClientConfig,
        mockMetricsRepository,
        mockIcProvider,
        false // isIngestionIsolation
    );

    // Verify
    verify(mockClientConfig).setMetricsRepository(mockMetricsRepository);
    zkClientFactoryMock.verify(() -> ZkClientFactory.newZkClient("localhost:2181"));
    verify(mockZkClient).subscribeStateChanges(any(ZkClientStatusStats.class));

    Assert.assertFalse(builder.isDaVinciClient());
    Assert.assertNotNull(builder.getStoreRepo());
    Assert.assertNotNull(builder.getSchemaRepo());
    Assert.assertNotNull(builder.getClusterInfoProvider());
    Assert.assertNotNull(builder.getLiveClusterConfigRepo());
    Assert.assertNotNull(builder.getZkClient());
    Assert.assertTrue(builder.getReadOnlyZKSharedSchemaRepository().isPresent());
  }

  @Test
  public void testConstructorWithServerAndIngestionIsolation() {
    // Setup
    when(mockServerConfig.isDaVinciClient()).thenReturn(false);
    when(mockClusterConfig.getZookeeperAddress()).thenReturn("localhost:2181");
    when(mockClusterConfig.getClusterName()).thenReturn("test-cluster");
    when(mockServerConfig.getSystemSchemaClusterName()).thenReturn("system-cluster");
    when(mockClusterConfig.getRefreshAttemptsForZkReconnect()).thenReturn(3);
    when(mockClusterConfig.getRefreshIntervalForZkReconnectInMs()).thenReturn(1000L);

    zkClientFactoryMock.when(() -> ZkClientFactory.newZkClient(anyString())).thenReturn(mockZkClient);

    // Execute
    VeniceMetadataRepositoryBuilder builder = new VeniceMetadataRepositoryBuilder(
        mockConfigLoader,
        mockClientConfig,
        mockMetricsRepository,
        mockIcProvider,
        true // isIngestionIsolation
    );

    // Verify - should use ingestion-isolation prefix for ZK client stats
    verify(mockZkClient).subscribeStateChanges(any(ZkClientStatusStats.class));

    Assert.assertFalse(builder.isDaVinciClient());
    Assert.assertNotNull(builder.getStoreRepo());
    Assert.assertNotNull(builder.getSchemaRepo());
    Assert.assertNotNull(builder.getClusterInfoProvider());
    Assert.assertNotNull(builder.getLiveClusterConfigRepo());
    Assert.assertNotNull(builder.getZkClient());
    Assert.assertTrue(builder.getReadOnlyZKSharedSchemaRepository().isPresent());
  }
}
