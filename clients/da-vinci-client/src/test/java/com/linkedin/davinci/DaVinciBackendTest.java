package com.linkedin.davinci;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES;
import static com.linkedin.venice.ConfigKeys.CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.DATA_BASE_PATH;
import static com.linkedin.venice.ConfigKeys.INGESTION_USE_DA_VINCI_CLIENT;
import static com.linkedin.venice.ConfigKeys.ZOOKEEPER_ADDRESS;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.DVC_INGESTION_ERROR_MEMORY_LIMIT_REACHED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.DVC_INGESTION_ERROR_OTHER;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.ERROR;
import static com.linkedin.venice.utils.DataProviderUtils.BOOLEAN;
import static com.linkedin.venice.utils.DataProviderUtils.allPermutationGenerator;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.fail;
import static org.testng.AssertJUnit.assertNull;

import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.repository.VeniceMetadataRepositoryBuilder;
import com.linkedin.davinci.store.cache.backend.ObjectCacheConfig;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.schema.StoreSchemaFetcher;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.exceptions.DiskLimitExhaustedException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.ZkClientFactory;
import com.linkedin.venice.meta.ClusterInfoProvider;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.SubscriptionBasedReadOnlyStoreRepository;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import com.linkedin.venice.serialization.avro.SchemaPresenceChecker;
import com.linkedin.venice.service.ICProvider;
import com.linkedin.venice.stats.ZkClientStatusStats;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class DaVinciBackendTest {
  private static final String STORE_NAME = "testStore";
  private static final Integer STORE_VERSION = 1;

  private DaVinciBackend backend;
  private MockedStatic<ClientFactory> mockClientFactory;
  private MockedConstruction<VeniceMetadataRepositoryBuilder> mockMetadataBuilder;
  private MockedConstruction<SchemaPresenceChecker> mockSchemaPresenceChecker;

  @BeforeClass
  public void init() {
    MockedStatic<ZkClientFactory> mockZkFactory = mockStatic(ZkClientFactory.class);
    ZkClient mockZkClient = mock(ZkClient.class);
    mockZkFactory.when(() -> ZkClientFactory.newZkClient(anyString())).thenReturn(mockZkClient);
    doNothing().when(mockZkClient).subscribeStateChanges(any(ZkClientStatusStats.class));
  }

  @BeforeMethod
  public void setUp() throws Exception {
    ClientConfig clientConfig = new ClientConfig(STORE_NAME).setVeniceURL("http://localhost:7777")
        .setMetricsRepository(new MetricsRepository());

    Properties serverProps = new Properties();
    serverProps.setProperty(CLUSTER_NAME, "test-cluster");
    serverProps.setProperty(ZOOKEEPER_ADDRESS, "localhost:2181");
    serverProps.setProperty("kafka.bootstrap.servers", "localhost:9092");
    serverProps.setProperty(INGESTION_USE_DA_VINCI_CLIENT, "true");
    serverProps.setProperty(DATA_BASE_PATH, "/tmp/test");
    serverProps.setProperty(ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES, "0");
    VeniceProperties veniceProperties = new VeniceProperties(serverProps);
    VeniceConfigLoader configLoader = new VeniceConfigLoader(veniceProperties);

    mockClientFactory = mockStatic(ClientFactory.class);
    SchemaReader mockSchemaReader = mock(SchemaReader.class);
    StoreSchemaFetcher mockSchemaFetcher = mock(StoreSchemaFetcher.class);

    mockClientFactory.when(() -> ClientFactory.getSchemaReader(any(ClientConfig.class), any()))
        .thenReturn(mockSchemaReader);
    mockClientFactory.when(() -> ClientFactory.createStoreSchemaFetcher(any(ClientConfig.class)))
        .thenReturn(mockSchemaFetcher);

    // Configure mock schema fetcher to return mock schema entries (prevent network calls)
    SchemaEntry mockValueSchemaEntry = mock(SchemaEntry.class);
    DerivedSchemaEntry mockUpdateSchemaEntry = mock(DerivedSchemaEntry.class);
    when(mockSchemaFetcher.getLatestValueSchemaEntry()).thenReturn(mockValueSchemaEntry);
    when(mockSchemaFetcher.getUpdateSchemaEntry(any(Integer.class))).thenReturn(mockUpdateSchemaEntry);
    when(mockValueSchemaEntry.getId()).thenReturn(1);

    // Mock VeniceMetadataRepositoryBuilder constructor to prevent metadata service calls
    ClusterInfoProvider mockClusterInfoProvider = mock(ClusterInfoProvider.class);
    SubscriptionBasedReadOnlyStoreRepository mockStoreRepository = mock(SubscriptionBasedReadOnlyStoreRepository.class);
    ReadOnlySchemaRepository mockSchemaRepository = mock(ReadOnlySchemaRepository.class);

    mockMetadataBuilder = mockConstruction(VeniceMetadataRepositoryBuilder.class, (mock, context) -> {
      when(mock.getClusterInfoProvider()).thenReturn(mockClusterInfoProvider);
      when(mock.getStoreRepo()).thenReturn(mockStoreRepository);
      when(mock.getSchemaRepo()).thenReturn(mockSchemaRepository);
    });

    // Mock SchemaPresenceChecker constructor to prevent schema validation network calls
    mockSchemaPresenceChecker = mockConstruction(SchemaPresenceChecker.class, (mock, context) -> {
      doNothing().when(mock).verifySchemaVersionPresentOrExit();
    });

    Optional<java.util.Set<String>> managedClients = Optional.empty();
    ICProvider mockICProvider = mock(ICProvider.class);
    Optional<ObjectCacheConfig> cacheConfig = Optional.empty();

    backend = new DaVinciBackend(clientConfig, configLoader, managedClients, mockICProvider, cacheConfig);
  }

  @AfterMethod
  public void cleanUp() {
    mockClientFactory.close();
    mockMetadataBuilder.close();
    mockSchemaPresenceChecker.close();
  }

  @DataProvider(name = "DvcErrorExecutionStatusAndBoolean")
  public static Object[][] dvcErrorExecutionStatusAndBoolean() {
    return allPermutationGenerator((permutation) -> {
      ExecutionStatus status = (ExecutionStatus) permutation[0];
      return status.isDVCIngestionError() && !status.equals(DVC_INGESTION_ERROR_MEMORY_LIMIT_REACHED);
    }, ExecutionStatus.values(), BOOLEAN);
  }

  @Test(dataProvider = "DvcErrorExecutionStatusAndBoolean")
  public void testGetDaVinciErrorStatus(
      ExecutionStatus executionStatus,
      boolean useDaVinciSpecificExecutionStatusForError) {
    VeniceException veniceException;
    switch (executionStatus) {
      case DVC_INGESTION_ERROR_DISK_FULL:
        veniceException = new DiskLimitExhaustedException("test");
        break;
      case DVC_INGESTION_ERROR_TOO_MANY_DEAD_INSTANCES:
      case DVC_INGESTION_ERROR_OTHER:
        veniceException = new VeniceException("test");
        break;
      default:
        fail("Unexpected execution status: " + executionStatus);
        return;
    }
    if (useDaVinciSpecificExecutionStatusForError) {
      assertEquals(
          DaVinciBackend.getDaVinciErrorStatus(veniceException, useDaVinciSpecificExecutionStatusForError),
          executionStatus.equals(ExecutionStatus.DVC_INGESTION_ERROR_TOO_MANY_DEAD_INSTANCES)
              ? DVC_INGESTION_ERROR_OTHER
              : executionStatus);
    } else {
      assertEquals(
          DaVinciBackend.getDaVinciErrorStatus(veniceException, useDaVinciSpecificExecutionStatusForError),
          ERROR);
    }
  }

  @Test(dataProvider = "DvcErrorExecutionStatusAndBoolean")
  public void testGetDaVinciErrorStatusNested(
      ExecutionStatus executionStatus,
      boolean useDaVinciSpecificExecutionStatusForError) {
    VeniceException veniceException;
    switch (executionStatus) {
      case DVC_INGESTION_ERROR_DISK_FULL:
        veniceException = new VeniceException(new DiskLimitExhaustedException("test"));
        break;
      case DVC_INGESTION_ERROR_TOO_MANY_DEAD_INSTANCES:
      case DVC_INGESTION_ERROR_OTHER:
        veniceException = new VeniceException("test");
        break;
      default:
        fail("Unexpected execution status: " + executionStatus);
        return;
    }
    if (useDaVinciSpecificExecutionStatusForError) {
      assertEquals(
          DaVinciBackend.getDaVinciErrorStatus(veniceException, useDaVinciSpecificExecutionStatusForError),
          executionStatus.equals(ExecutionStatus.DVC_INGESTION_ERROR_TOO_MANY_DEAD_INSTANCES)
              ? DVC_INGESTION_ERROR_OTHER
              : executionStatus);
    } else {
      assertEquals(
          DaVinciBackend.getDaVinciErrorStatus(veniceException, useDaVinciSpecificExecutionStatusForError),
          ERROR);
    }
  }

  @Test(dataProvider = "DvcErrorExecutionStatusAndBoolean")
  public void testGetDaVinciErrorStatusWithInvalidCases(
      ExecutionStatus executionStatus,
      boolean useDaVinciSpecificExecutionStatusForError) {
    VeniceException veniceException;
    switch (executionStatus) {
      case DVC_INGESTION_ERROR_DISK_FULL:
      case DVC_INGESTION_ERROR_TOO_MANY_DEAD_INSTANCES:
      case DVC_INGESTION_ERROR_OTHER:
        veniceException = new VeniceException("test");
        break;
      default:
        fail("Unexpected execution status: " + executionStatus);
        return;
    }

    if (useDaVinciSpecificExecutionStatusForError) {
      assertEquals(
          DaVinciBackend.getDaVinciErrorStatus(veniceException, useDaVinciSpecificExecutionStatusForError),
          DVC_INGESTION_ERROR_OTHER);

    } else {
      assertEquals(
          DaVinciBackend.getDaVinciErrorStatus(veniceException, useDaVinciSpecificExecutionStatusForError),
          ERROR);
    }
  }

  @Test
  public void testBootstrappingAwareCompletableFuture()
      throws ExecutionException, InterruptedException, TimeoutException {
    DaVinciBackend backend = mock(DaVinciBackend.class);

    when(backend.hasCurrentVersionBootstrapping()).thenReturn(true).thenReturn(false);

    DaVinciBackend.BootstrappingAwareCompletableFuture future =
        new DaVinciBackend.BootstrappingAwareCompletableFuture(backend);
    future.getBootstrappingFuture().get(10, TimeUnit.SECONDS);
    verify(backend, times(2)).hasCurrentVersionBootstrapping();
  }

  @Test
  public void testRegisterStoreClients() {
    // Register regular client
    backend.registerStoreClient(STORE_NAME, null);
    assertEquals(backend.getStoreClientType(STORE_NAME), DaVinciBackend.ClientType.REGULAR);

    // Register version-specific client
    String versionSpecificStoreName = "test-store-1";
    Integer storeVersion = 1;
    backend.registerStoreClient(versionSpecificStoreName, storeVersion);
    assertEquals(backend.getStoreClientType(versionSpecificStoreName), DaVinciBackend.ClientType.VERSION_SPECIFIC);
    assertEquals(backend.getVersionSpecificStoreVersion(versionSpecificStoreName), storeVersion);

    // Register another regular client
    String storeName2 = "test-store-2";
    backend.registerStoreClient(storeName2, null);
    assertEquals(backend.getStoreClientType(storeName2), DaVinciBackend.ClientType.REGULAR);

    // Register same version-specific client twice
    backend.registerStoreClient(versionSpecificStoreName, storeVersion);
    assertEquals(backend.getVersionSpecificStoreVersion(versionSpecificStoreName), storeVersion);
    assertEquals(backend.getStoreClientType(versionSpecificStoreName), DaVinciBackend.ClientType.VERSION_SPECIFIC);

    // Unregister clients and verify state cleanup
    backend.unregisterStoreClient(STORE_NAME, null);
    backend.unregisterStoreClient(versionSpecificStoreName, storeVersion);
    assertNull(backend.getStoreClientType(STORE_NAME));
    assertNull(backend.getStoreClientType(versionSpecificStoreName));
    assertNull(backend.getVersionSpecificStoreVersion(versionSpecificStoreName));
  }

  @Test
  public void testRegisterStoreClientExceptions() {
    // Version-specific client after regular should throw
    backend.registerStoreClient(STORE_NAME, null);
    assertThrows(VeniceClientException.class, () -> backend.registerStoreClient(STORE_NAME, STORE_VERSION));
    backend.unregisterStoreClient(STORE_NAME, null);

    // Regular client after version-specific should throw
    backend.registerStoreClient(STORE_NAME, STORE_VERSION);
    assertThrows(VeniceClientException.class, () -> backend.registerStoreClient(STORE_NAME, null));
    backend.unregisterStoreClient(STORE_NAME, null);

    // Different versions for same store should throw
    backend.registerStoreClient(STORE_NAME, STORE_VERSION);
    assertThrows(VeniceClientException.class, () -> backend.registerStoreClient(STORE_NAME, 2));
  }

  @Test
  public void testHandleStoreChanged() {
    StoreBackend storeBackend = mock(StoreBackend.class);
    when(storeBackend.getStoreName()).thenReturn(STORE_NAME);

    // Regular client should respect version swap
    backend.registerStoreClient(STORE_NAME, null);
    backend.handleStoreChanged(storeBackend);
    verify(storeBackend).validateDaVinciAndVeniceCurrentVersion();
    verify(storeBackend).tryDeleteInvalidDaVinciFutureVersion();
    verify(storeBackend).trySwapDaVinciCurrentVersion(null);
    verify(storeBackend).trySubscribeDaVinciFutureVersion();

    clearInvocations(storeBackend);
    backend.unregisterStoreClient(STORE_NAME, null);

    // Version specific client should ignore version swap
    backend.registerStoreClient(STORE_NAME, STORE_VERSION);
    backend.handleStoreChanged(storeBackend);
    verify(storeBackend, never()).validateDaVinciAndVeniceCurrentVersion();
    verify(storeBackend, never()).tryDeleteInvalidDaVinciFutureVersion();
    verify(storeBackend, never()).trySwapDaVinciCurrentVersion(null);
    verify(storeBackend, never()).trySubscribeDaVinciFutureVersion();
  }
}
