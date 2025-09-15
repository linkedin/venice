package com.linkedin.davinci;

import static com.linkedin.venice.ConfigKeys.DA_VINCI_SUBSCRIBE_ON_DISK_PARTITIONS_AUTOMATICALLY;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.DVC_INGESTION_ERROR_MEMORY_LIMIT_REACHED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.DVC_INGESTION_ERROR_OTHER;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.ERROR;
import static com.linkedin.venice.utils.DataProviderUtils.BOOLEAN;
import static com.linkedin.venice.utils.DataProviderUtils.allPermutationGenerator;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import static org.testng.AssertJUnit.assertNull;

import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.repository.VeniceMetadataRepositoryBuilder;
import com.linkedin.davinci.storage.StorageEngineRepository;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.davinci.store.cache.backend.ObjectCacheConfig;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.schema.StoreSchemaFetcher;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.exceptions.DiskLimitExhaustedException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.ClusterInfoProvider;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.SubscriptionBasedReadOnlyStoreRepository;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import com.linkedin.venice.serialization.avro.SchemaPresenceChecker;
import com.linkedin.venice.service.ICProvider;
import com.linkedin.venice.utils.ComplementSet;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class DaVinciBackendTest {
  private static final String STORE_NAME = "testStore";
  private static final Integer STORE_VERSION = 1;

  private DaVinciBackend backend;
  private MockedStatic<ClientFactory> mockedClientFactory;
  private MockedConstruction<VeniceMetadataRepositoryBuilder> mockedMetadataBuilder;
  private MockedConstruction<SchemaPresenceChecker> mockedSchemaPresenceChecker;

  @BeforeMethod
  public void setUp() throws Exception {
    ClientConfig realClientConfig = new ClientConfig(STORE_NAME).setVeniceURL("http://localhost:7777")
        .setMetricsRepository(new MetricsRepository());

    Properties serverProps = new Properties();
    serverProps.setProperty("cluster.name", "test-cluster");
    serverProps.setProperty("zookeeper.address", "localhost:2181");
    serverProps.setProperty("kafka.bootstrap.servers", "localhost:9092");
    serverProps.setProperty("ingestion.use.da.vinci.client", "true");
    serverProps.setProperty("data.base.path", "/tmp/test");
    serverProps.setProperty("davinci.push.status.check.interval.in.ms", "1000");
    VeniceProperties veniceProperties = new VeniceProperties(serverProps);
    VeniceConfigLoader configLoader = new VeniceConfigLoader(veniceProperties);

    mockedClientFactory = mockStatic(ClientFactory.class);
    SchemaReader mockSchemaReader = mock(SchemaReader.class);
    StoreSchemaFetcher mockSchemaFetcher = mock(StoreSchemaFetcher.class);

    mockedClientFactory.when(() -> ClientFactory.getSchemaReader(any(ClientConfig.class), any()))
        .thenReturn(mockSchemaReader);
    mockedClientFactory.when(() -> ClientFactory.createStoreSchemaFetcher(any(ClientConfig.class)))
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

    mockedMetadataBuilder = mockConstruction(VeniceMetadataRepositoryBuilder.class, (mock, context) -> {
      when(mock.getClusterInfoProvider()).thenReturn(mockClusterInfoProvider);
      when(mock.getStoreRepo()).thenReturn(mockStoreRepository);
      when(mock.getSchemaRepo()).thenReturn(mockSchemaRepository);
    });

    // Mock SchemaPresenceChecker constructor to prevent schema validation network calls
    mockedSchemaPresenceChecker = mockConstruction(SchemaPresenceChecker.class, (mock, context) -> {
      doNothing().when(mock).verifySchemaVersionPresentOrExit();
    });

    Optional<java.util.Set<String>> managedClients = Optional.empty();
    ICProvider mockICProvider = mock(ICProvider.class);
    Optional<ObjectCacheConfig> cacheConfig = Optional.empty();

    backend = new DaVinciBackend(realClientConfig, configLoader, managedClients, mockICProvider, cacheConfig);
  }

  @AfterMethod
  public void cleanUp() {
    if (mockedClientFactory != null) {
      mockedClientFactory.close();
    }
    if (mockedMetadataBuilder != null) {
      mockedMetadataBuilder.close();
    }
    if (mockedSchemaPresenceChecker != null) {
      mockedSchemaPresenceChecker.close();
    }
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
  public void testBootstrappingSubscription() {
    StorageService mockStorageService = mock(StorageService.class);

    StorageEngineRepository mockStorageEngineRepository = mock(StorageEngineRepository.class);
    StorageEngine storageEngine = mock(StorageEngine.class);
    String resourceName = "test_store_v1";
    when(storageEngine.getStoreVersionName()).thenReturn(resourceName);

    List<StorageEngine> localStorageEngines = new ArrayList<>();
    localStorageEngines.add(storageEngine);

    // Mock storage service behavior
    when(mockStorageService.getStorageEngineRepository()).thenReturn(mockStorageEngineRepository);
    when(mockStorageService.getStorageEngine(resourceName)).thenReturn(storageEngine);
    when(mockStorageEngineRepository.getAllLocalStorageEngines()).thenReturn(localStorageEngines);

    List<Integer> userPartitionList = new ArrayList<>();
    userPartitionList.add(0);
    userPartitionList.add(1);
    userPartitionList.add(2);
    when(mockStorageService.getUserPartitions(resourceName)).thenReturn(userPartitionList);

    HashSet<Integer> backendSubscription = new HashSet<>();
    backendSubscription.add(0);
    backendSubscription.add(1);

    StoreBackend mockStoreBackend = mock(StoreBackend.class);
    ComplementSet<Integer> backendSubscriptionSet = ComplementSet.wrap(backendSubscription);
    when(mockStoreBackend.getSubscription()).thenReturn(backendSubscriptionSet);

    doAnswer(invocation -> {
      ComplementSet<Integer> partitions = invocation.getArgument(0);
      mockStoreBackend.getSubscription().addAll(partitions);
      return null;
    }).when(mockStoreBackend).subscribe(any(), any());

    Version mockVersion = mock(Version.class);
    when(mockVersion.kafkaTopicName()).thenReturn(resourceName);

    Store mockStore = mock(Store.class);
    SubscriptionBasedReadOnlyStoreRepository mockStoreRepository = mock(SubscriptionBasedReadOnlyStoreRepository.class);
    when(mockStoreRepository.getStoreOrThrow(Version.parseStoreFromKafkaTopicName(resourceName))).thenReturn(mockStore);
    when(mockStore.getVersion(Version.parseVersionFromKafkaTopicName(resourceName))).thenReturn(mockVersion);

    backend.setStoreRepository(mockStoreRepository);
    backend.setStorageService(mockStorageService);
    backend.addStoreBackend(Version.parseStoreFromKafkaTopicName(resourceName), mockStoreBackend);

    // DA_VINCI_SUBSCRIBE_ON_DISK_PARTITIONS_AUTOMATICALLY == false
    Properties testProps = backend.getConfigLoader().getCombinedProperties().toProperties();
    testProps.setProperty(DA_VINCI_SUBSCRIBE_ON_DISK_PARTITIONS_AUTOMATICALLY, "false");
    VeniceProperties updatedProperties = new VeniceProperties(testProps);
    VeniceConfigLoader updatedConfigLoader = new VeniceConfigLoader(updatedProperties);
    backend.setConfigLoader(updatedConfigLoader);
    backend.bootstrap();

    ComplementSet<Integer> subscription = mockStoreBackend.getSubscription();
    assertTrue(subscription.contains(0));
    assertTrue(subscription.contains(1));
    assertFalse(subscription.contains(2));

    // DA_VINCI_SUBSCRIBE_ON_DISK_PARTITIONS_AUTOMATICALLY == true
    testProps = backend.getConfigLoader().getCombinedProperties().toProperties();
    testProps.setProperty(DA_VINCI_SUBSCRIBE_ON_DISK_PARTITIONS_AUTOMATICALLY, "true");
    VeniceProperties updatedProperties2 = new VeniceProperties(testProps);
    VeniceConfigLoader updatedConfigLoader2 = new VeniceConfigLoader(updatedProperties2);
    backend.setConfigLoader(updatedConfigLoader2);
    backend.bootstrap();

    subscription = mockStoreBackend.getSubscription();
    assertTrue(subscription.contains(0));
    assertTrue(subscription.contains(1));
    assertTrue(subscription.contains(2));
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
}
