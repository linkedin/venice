package com.linkedin.davinci.kafka.consumer;

import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atMostOnce;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.compression.StorageEngineBackedCompressorFactory;
import com.linkedin.davinci.config.VeniceClusterConfig;
import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.davinci.store.AbstractStorageEngineTest;
import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.ClusterInfoProvider;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadOnlyLiveClusterConfigRepository;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.ReadStrategy;
import com.linkedin.venice.meta.RoutingStrategy;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreVersionInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.meta.ZKStore;
import com.linkedin.venice.pubsub.PubSubAdminAdapterFactory;
import com.linkedin.venice.pubsub.PubSubClientsFactory;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterContext;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterFactory;
import com.linkedin.venice.pubsub.PubSubPositionTypeRegistry;
import com.linkedin.venice.pubsub.PubSubProducerAdapterFactory;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubSecurityProtocol;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.service.ICProvider;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.ReferenceCounted;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.locks.ResourceAutoClosableLockManager;
import io.tehuti.metrics.MetricsRepository;
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import it.unimi.dsi.fastutil.objects.Object2IntMaps;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import org.apache.avro.Schema;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


@Test
public abstract class KafkaStoreIngestionServiceTest {
  private StorageService mockStorageService;
  private VeniceConfigLoader mockVeniceConfigLoader;
  private StorageMetadataService storageMetadataService;
  private ClusterInfoProvider mockClusterInfoProvider;
  private ReadOnlyStoreRepository mockMetadataRepo;
  private ReadOnlySchemaRepository mockSchemaRepo;
  private ReadOnlyLiveClusterConfigRepository mockLiveClusterConfigRepo;
  private PubSubClientsFactory mockPubSubClientsFactory;
  private StorageEngineBackedCompressorFactory compressorFactory;

  private KafkaStoreIngestionService kafkaStoreIngestionService;

  private PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();

  @BeforeMethod(alwaysRun = true)
  public void setUp() {
    mockStorageService = mock(StorageService.class);
    doReturn(new ReferenceCounted<>(mock(StorageEngine.class), se -> {})).when(mockStorageService)
        .getRefCountedStorageEngine(anyString());
    storageMetadataService = mock(StorageMetadataService.class);
    mockClusterInfoProvider = mock(ClusterInfoProvider.class);
    mockMetadataRepo = mock(ReadOnlyStoreRepository.class);
    mockSchemaRepo = mock(ReadOnlySchemaRepository.class);
    mockLiveClusterConfigRepo = mock(ReadOnlyLiveClusterConfigRepository.class);
    PubSubConsumerAdapterFactory mockPubSubConsumerAdapterFactory = mock(PubSubConsumerAdapterFactory.class);
    doReturn(mock(PubSubConsumerAdapter.class)).when(mockPubSubConsumerAdapterFactory)
        .create(any(PubSubConsumerAdapterContext.class));
    mockPubSubClientsFactory = new PubSubClientsFactory(
        mock(PubSubProducerAdapterFactory.class),
        mockPubSubConsumerAdapterFactory,
        mock(PubSubAdminAdapterFactory.class));
    compressorFactory = new StorageEngineBackedCompressorFactory(storageMetadataService);

    setupMockConfig();
  }

  abstract KafkaConsumerService.ConsumerAssignmentStrategy getConsumerAssignmentStrategy();

  private void setupMockConfig() {
    mockVeniceConfigLoader = mock(VeniceConfigLoader.class);
    String dummyKafkaUrl = "localhost:16637";

    VeniceServerConfig mockVeniceServerConfig = mock(VeniceServerConfig.class);
    doReturn(-1L).when(mockVeniceServerConfig).getKafkaFetchQuotaBytesPerSecond();
    doReturn(-1L).when(mockVeniceServerConfig).getKafkaFetchQuotaRecordPerSecond();
    doReturn(-1L).when(mockVeniceServerConfig).getKafkaFetchQuotaUnorderedBytesPerSecond();
    doReturn(-1L).when(mockVeniceServerConfig).getKafkaFetchQuotaUnorderedRecordPerSecond();
    doReturn("").when(mockVeniceServerConfig).getDataBasePath();
    doReturn(0.9d).when(mockVeniceServerConfig).getDiskFullThreshold();
    doReturn(Int2ObjectMaps.emptyMap()).when(mockVeniceServerConfig).getKafkaClusterIdToAliasMap();
    doReturn(Object2IntMaps.emptyMap()).when(mockVeniceServerConfig).getKafkaClusterUrlToIdMap();
    doReturn(KafkaConsumerServiceDelegator.ConsumerPoolStrategyType.DEFAULT).when(mockVeniceServerConfig)
        .getConsumerPoolStrategyType();
    doReturn(2).when(mockVeniceServerConfig).getAaWCIngestionStorageLookupThreadPoolSize();
    doReturn(1).when(mockVeniceServerConfig).getStoreWriterNumber();
    doReturn(5).when(mockVeniceServerConfig).getIdleIngestionTaskCleanupIntervalInSeconds();

    // Consumer related configs for preparing kafka consumer service.
    doReturn(dummyKafkaUrl).when(mockVeniceServerConfig).getKafkaBootstrapServers();
    Function<String, String> kafkaClusterUrlResolver = String::toString;
    doReturn(kafkaClusterUrlResolver).when(mockVeniceServerConfig).getKafkaClusterUrlResolver();
    doReturn(VeniceProperties.empty()).when(mockVeniceServerConfig).getKafkaConsumerConfigsForLocalConsumption();
    doReturn(getConsumerAssignmentStrategy()).when(mockVeniceServerConfig).getSharedConsumerAssignmentStrategy();
    doReturn(1).when(mockVeniceServerConfig).getConsumerPoolSizePerKafkaCluster();
    doReturn(PubSubSecurityProtocol.PLAINTEXT).when(mockVeniceServerConfig).getPubSubSecurityProtocol(dummyKafkaUrl);
    doReturn(10).when(mockVeniceServerConfig).getKafkaMaxPollRecords();
    doReturn(2).when(mockVeniceServerConfig).getTopicManagerMetadataFetcherConsumerPoolSize();
    doReturn(2).when(mockVeniceServerConfig).getTopicManagerMetadataFetcherThreadPoolSize();
    doReturn(30l).when(mockVeniceServerConfig).getKafkaFetchQuotaTimeWindow();
    doReturn(PubSubPositionTypeRegistry.RESERVED_POSITION_TYPE_REGISTRY).when(mockVeniceServerConfig)
        .getPubSubPositionTypeRegistry();

    VeniceClusterConfig mockVeniceClusterConfig = mock(VeniceClusterConfig.class);
    Properties properties = new Properties();
    properties.put(KAFKA_BOOTSTRAP_SERVERS, dummyKafkaUrl);
    VeniceProperties mockVeniceProperties = new VeniceProperties(properties);
    doReturn(mockVeniceProperties).when(mockVeniceClusterConfig).getClusterProperties();
    doReturn(mockVeniceProperties).when(mockVeniceServerConfig).getClusterProperties();

    doReturn(mockVeniceServerConfig).when(mockVeniceConfigLoader).getVeniceServerConfig();
    doReturn(mockVeniceClusterConfig).when(mockVeniceConfigLoader).getVeniceClusterConfig();
  }

  @Test
  public void testDisableMetricsEmission() {
    kafkaStoreIngestionService = new KafkaStoreIngestionService(
        mockStorageService,
        mockVeniceConfigLoader,
        storageMetadataService,
        mockClusterInfoProvider,
        mockMetadataRepo,
        mockSchemaRepo,
        mockLiveClusterConfigRepo,
        new MetricsRepository(),
        Optional.empty(),
        Optional.empty(),
        AvroProtocolDefinition.PARTITION_STATE.getSerializer(),
        Optional.empty(),
        null,
        false,
        compressorFactory,
        Optional.empty(),
        null,
        false,
        null,
        mockPubSubClientsFactory,
        Optional.empty(),
        null,
        null,
        null);

    String mockStoreName = "test";
    String mockSimilarStoreName = "testTest";
    /**
    * Explicitly make max version number higher than the largest version push in ingestion service;
    * it's possible that user starts the push for version 4 and 5 but the future version pushes fail.
    *
    * In this case, ingestion service should emit metrics for the known largest version push in server.
    */
    int taskNum = 3;
    int maxVersionNumber = 5;
    NavigableMap<String, StoreIngestionTask> topicNameToIngestionTaskMap = new ConcurrentSkipListMap<>();

    for (int i = 1; i <= taskNum; i++) {
      StoreIngestionTask task = mock(StoreIngestionTask.class);
      topicNameToIngestionTaskMap.put(mockStoreName + "_v" + String.valueOf(i), task);
    }

    topicNameToIngestionTaskMap.put(mockSimilarStoreName + "_v1", mock(StoreIngestionTask.class));

    Store mockStore = mock(Store.class);
    doReturn(maxVersionNumber).when(mockStore).getLargestUsedVersionNumber();
    doReturn(mockStore).when(mockMetadataRepo).getStore(mockStoreName);

    VeniceStoreVersionConfig mockStoreConfig = mock(VeniceStoreVersionConfig.class);
    doReturn(mockStoreName + "_v" + String.valueOf(taskNum)).when(mockStoreConfig).getStoreVersionName();

    kafkaStoreIngestionService.updateStatsEmission(topicNameToIngestionTaskMap, mockStoreName, maxVersionNumber);

    String mostRecentTopic = mockStoreName + "_v" + taskNum;
    topicNameToIngestionTaskMap.forEach((topicName, task) -> {
      if (Version.parseStoreFromKafkaTopicName(topicName).equals(mockStoreName)) {
        if (topicName.equals(mostRecentTopic)) {
          verify(task).enableMetricsEmission();
        } else {
          verify(task).disableMetricsEmission();
        }
      } else { // checks store with similar name will not be call
        verify(task, never()).enableMetricsEmission();
        verify(task, never()).disableMetricsEmission();
      }
    });

    /**
    * Test when the latest push job for mock store is killed; the previous latest ongoing push job should enable
    * metrics emission.
    */
    topicNameToIngestionTaskMap.remove(mostRecentTopic);
    kafkaStoreIngestionService.updateStatsEmission(topicNameToIngestionTaskMap, mockStoreName);
    String latestOngoingPushJob = mockStoreName + "_v" + (taskNum - 1);
    topicNameToIngestionTaskMap.forEach((topicName, task) -> {
      if (topicName.equals(latestOngoingPushJob)) {
        verify(task).enableMetricsEmission();
      }
    });
    kafkaStoreIngestionService.close();
  }

  @Test
  public void testGetIngestingTopicsNotWithOnlineVersion() {
    // Without starting the ingestion service test getIngestingTopicsWithVersionStatusNotOnline would return the correct
    // topics under different scenarios.
    kafkaStoreIngestionService = new KafkaStoreIngestionService(
        mockStorageService,
        mockVeniceConfigLoader,
        storageMetadataService,
        mockClusterInfoProvider,
        mockMetadataRepo,
        mockSchemaRepo,
        mockLiveClusterConfigRepo,
        new MetricsRepository(),
        Optional.empty(),
        Optional.empty(),
        AvroProtocolDefinition.PARTITION_STATE.getSerializer(),
        Optional.empty(),
        null,
        false,
        compressorFactory,
        Optional.empty(),
        null,
        false,
        null,
        mockPubSubClientsFactory,
        Optional.empty(),
        null,
        null,
        null);
    String topic1 = "test-store_v1";
    String topic2 = "test-store_v2";
    String invalidTopic = "invalid-store_v1";
    String storeName = Version.parseStoreFromKafkaTopicName(topic1);
    String deletedStoreName = Version.parseStoreFromKafkaTopicName(invalidTopic);
    Store mockStore = new ZKStore(
        storeName,
        "unit-test",
        0,
        PersistenceType.ROCKS_DB,
        RoutingStrategy.CONSISTENT_HASH,
        ReadStrategy.ANY_OF_ONLINE,
        OfflinePushStrategy.WAIT_ALL_REPLICAS,
        1);
    Store toBeDeletedStore = new ZKStore(
        deletedStoreName,
        "unit-test",
        0,
        PersistenceType.ROCKS_DB,
        RoutingStrategy.CONSISTENT_HASH,
        ReadStrategy.ANY_OF_ONLINE,
        OfflinePushStrategy.WAIT_ALL_REPLICAS,
        1);
    mockStore.addVersion(new VersionImpl(storeName, 1, "test-job-id"));
    toBeDeletedStore.addVersion(new VersionImpl(deletedStoreName, 1, "test-job-id"));
    doReturn(mockStore).when(mockMetadataRepo).getStore(storeName);
    doReturn(toBeDeletedStore).when(mockMetadataRepo).getStore(deletedStoreName);
    doReturn(mockStore).when(mockMetadataRepo).getStoreOrThrow(storeName);
    doReturn(toBeDeletedStore).when(mockMetadataRepo).getStoreOrThrow(deletedStoreName);
    doReturn(new StoreVersionInfo(mockStore, mockStore.getVersion(1))).when(mockMetadataRepo)
        .waitVersion(eq(storeName), eq(1), any());
    doReturn(new StoreVersionInfo(toBeDeletedStore, toBeDeletedStore.getVersion(1))).when(mockMetadataRepo)
        .waitVersion(eq(deletedStoreName), eq(1), any());
    VeniceProperties veniceProperties = AbstractStorageEngineTest.getServerProperties(PersistenceType.ROCKS_DB);
    kafkaStoreIngestionService.startConsumption(new VeniceStoreVersionConfig(topic1, veniceProperties), 0);
    assertEquals(
        kafkaStoreIngestionService.getIngestingTopicsWithVersionStatusNotOnline().size(),
        1,
        "Unexpected number of ingesting topics with version status of not ONLINE");
    mockStore.updateVersionStatus(1, VersionStatus.ONLINE);
    assertEquals(
        kafkaStoreIngestionService.getIngestingTopicsWithVersionStatusNotOnline().size(),
        0,
        "Expecting an empty set since all ingesting topics have version status of ONLINE");
    mockStore.addVersion(new VersionImpl(storeName, 2, "test-job-id"));
    doReturn(new StoreVersionInfo(mockStore, mockStore.getVersion(2))).when(mockMetadataRepo)
        .waitVersion(eq(storeName), eq(2), any());
    kafkaStoreIngestionService.startConsumption(new VeniceStoreVersionConfig(topic2, veniceProperties), 0);
    kafkaStoreIngestionService.startConsumption(new VeniceStoreVersionConfig(invalidTopic, veniceProperties), 0);
    doThrow(new VeniceNoStoreException(deletedStoreName)).when(mockMetadataRepo).getStoreOrThrow(deletedStoreName);
    doReturn(null).when(mockMetadataRepo).getStore(deletedStoreName);
    assertEquals(
        kafkaStoreIngestionService.getIngestingTopicsWithVersionStatusNotOnline().size(),
        2,
        "Invalid and in flight ingesting topics should be included in the returned set");
    mockStore.updateVersionStatus(2, VersionStatus.ONLINE);
    mockStore.deleteVersion(1);
    Set<String> results = kafkaStoreIngestionService.getIngestingTopicsWithVersionStatusNotOnline();
    assertTrue(
        results.size() == 2 && results.contains(invalidTopic) && results.contains(topic1),
        "Invalid and retired ingesting topics should be included in the returned set");
    kafkaStoreIngestionService.close();
  }

  @Test
  public void testCloseStoreIngestionTask() {
    kafkaStoreIngestionService = new KafkaStoreIngestionService(
        mockStorageService,
        mockVeniceConfigLoader,
        storageMetadataService,
        mockClusterInfoProvider,
        mockMetadataRepo,
        mockSchemaRepo,
        mockLiveClusterConfigRepo,
        new MetricsRepository(),
        Optional.empty(),
        Optional.empty(),
        AvroProtocolDefinition.PARTITION_STATE.getSerializer(),
        Optional.empty(),
        null,
        false,
        compressorFactory,
        Optional.empty(),
        null,
        false,
        null,
        mockPubSubClientsFactory,
        Optional.empty(),
        null,
        null,
        null);
    String topicName = "test-store_v1";
    String storeName = Version.parseStoreFromKafkaTopicName(topicName);
    Store mockStore = new ZKStore(
        storeName,
        "unit-test",
        0,
        PersistenceType.ROCKS_DB,
        RoutingStrategy.CONSISTENT_HASH,
        ReadStrategy.ANY_OF_ONLINE,
        OfflinePushStrategy.WAIT_ALL_REPLICAS,
        1);

    SchemaEntry mockSchemaEntry = Mockito.mock(SchemaEntry.class);
    Mockito.when(mockSchemaEntry.getSchema()).thenReturn(Schema.create(Schema.Type.STRING));
    Mockito.when(mockSchemaRepo.getKeySchema(topicName)).thenReturn(Mockito.mock(SchemaEntry.class));

    StorageEngine storageEngine1 = mock(StorageEngine.class);
    doReturn(new ReferenceCounted<>(storageEngine1, se -> {})).when(mockStorageService)
        .getRefCountedStorageEngine(topicName);

    mockStore.addVersion(new VersionImpl(storeName, 1, "test-job-id"));
    doReturn(mockStore).when(mockMetadataRepo).getStore(storeName);
    doReturn(mockStore).when(mockMetadataRepo).getStoreOrThrow(storeName);
    doReturn(new StoreVersionInfo(mockStore, mockStore.getVersion(1))).when(mockMetadataRepo)
        .waitVersion(eq(storeName), eq(1), any());
    VeniceProperties veniceProperties = AbstractStorageEngineTest.getServerProperties(PersistenceType.ROCKS_DB);
    kafkaStoreIngestionService.startConsumption(new VeniceStoreVersionConfig(topicName, veniceProperties), 0);
    StoreIngestionTask storeIngestionTask = kafkaStoreIngestionService.getStoreIngestionTask(topicName);
    kafkaStoreIngestionService.shutdownStoreIngestionTask(topicName);
    StoreIngestionTask closedStoreIngestionTask = kafkaStoreIngestionService.getStoreIngestionTask(topicName);
    assertNull(closedStoreIngestionTask);

    StorageEngine storageEngine2 = mock(StorageEngine.class);
    doReturn(new ReferenceCounted<>(storageEngine2, se -> {})).when(mockStorageService)
        .getRefCountedStorageEngine(topicName);
    kafkaStoreIngestionService.startConsumption(new VeniceStoreVersionConfig(topicName, veniceProperties), 0);
    StoreIngestionTask newStoreIngestionTask = kafkaStoreIngestionService.getStoreIngestionTask(topicName);
    Assert.assertNotNull(newStoreIngestionTask);
    Assert.assertNotEquals(storeIngestionTask, newStoreIngestionTask);
    assertEquals(newStoreIngestionTask.getStorageEngine(), storageEngine2);

    // Mimic a graceful shutdown timeout
    kafkaStoreIngestionService.startConsumption(new VeniceStoreVersionConfig(topicName, veniceProperties), 0);
    StoreIngestionTask shutdownTimeoutTask = kafkaStoreIngestionService.getStoreIngestionTask(topicName);
    // Initialize the latch forcefully to mimic task is running
    shutdownTimeoutTask.getGracefulShutdownLatch().get();
    // Graceful shutdown wait should time out
    Assert.assertFalse(shutdownTimeoutTask.shutdownAndWait(1));
    kafkaStoreIngestionService.close();
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testStoreIngestionTaskShutdownLastPartition(boolean isIsolatedIngestion) {
    kafkaStoreIngestionService = new KafkaStoreIngestionService(
        mockStorageService,
        mockVeniceConfigLoader,
        storageMetadataService,
        mockClusterInfoProvider,
        mockMetadataRepo,
        mockSchemaRepo,
        mockLiveClusterConfigRepo,
        new MetricsRepository(),
        Optional.empty(),
        Optional.empty(),
        AvroProtocolDefinition.PARTITION_STATE.getSerializer(),
        Optional.empty(),
        null,
        isIsolatedIngestion,
        compressorFactory,
        Optional.empty(),
        null,
        false,
        null,
        mockPubSubClientsFactory,
        Optional.empty(),
        null,
        null,
        null);
    String topicName = "test-store_v1";
    String storeName = Version.parseStoreFromKafkaTopicName(topicName);
    Store mockStore = new ZKStore(
        storeName,
        "unit-test",
        0,
        PersistenceType.ROCKS_DB,
        RoutingStrategy.CONSISTENT_HASH,
        ReadStrategy.ANY_OF_ONLINE,
        OfflinePushStrategy.WAIT_ALL_REPLICAS,
        1);

    mockStore.addVersion(new VersionImpl(storeName, 1, "test-job-id"));
    doReturn(mockStore).when(mockMetadataRepo).getStore(storeName);
    doReturn(mockStore).when(mockMetadataRepo).getStoreOrThrow(storeName);
    doReturn(new StoreVersionInfo(mockStore, mockStore.getVersion(1))).when(mockMetadataRepo)
        .waitVersion(eq(storeName), eq(1), any());
    VeniceProperties veniceProperties = AbstractStorageEngineTest.getServerProperties(PersistenceType.ROCKS_DB);
    VeniceStoreVersionConfig config = new VeniceStoreVersionConfig(topicName, veniceProperties);
    kafkaStoreIngestionService.startConsumption(config, 0);
    kafkaStoreIngestionService.stopConsumptionAndWait(config, 0, 1, 1, true);
    StoreIngestionTask storeIngestionTask = kafkaStoreIngestionService.getStoreIngestionTask(topicName);
    if (isIsolatedIngestion) {
      Assert.assertNotNull(storeIngestionTask);
    } else {
      assertNull(storeIngestionTask);
    }
    kafkaStoreIngestionService.startConsumption(config, 0);
    storeIngestionTask = kafkaStoreIngestionService.getStoreIngestionTask(topicName);
    storeIngestionTask.setPartitionConsumptionState(1, mock(PartitionConsumptionState.class));
    kafkaStoreIngestionService.stopConsumptionAndWait(config, 0, 1, 1, true);
    Assert.assertNotNull(storeIngestionTask);
    PubSubTopic pubSubTopic = pubSubTopicRepository.getTopic(topicName);
    PubSubTopicPartition pubSubTopicPartition = new PubSubTopicPartitionImpl(pubSubTopic, 0);
    Properties consumerProperties = new Properties();
    consumerProperties.put(KAFKA_BOOTSTRAP_SERVERS, "localhost:16637");
    AbstractKafkaConsumerService kafkaConsumerService =
        spy(storeIngestionTask.aggKafkaConsumerService.createKafkaConsumerService(consumerProperties));
    kafkaStoreIngestionService.getTopicPartitionIngestionContext(topicName, topicName, 0);
    verify(kafkaConsumerService, atMostOnce()).getIngestionInfoFor(pubSubTopic, pubSubTopicPartition);
    kafkaStoreIngestionService.close();
  }

  @Test
  public void testHasCurrentVersionBootstrapping() {
    StoreIngestionTask nonCurrentVersionBootstrappingTask = mock(StoreIngestionTask.class);
    doReturn(false).when(nonCurrentVersionBootstrappingTask).isCurrentVersion();
    doReturn(false).when(nonCurrentVersionBootstrappingTask).hasAllPartitionReportedCompleted();

    StoreIngestionTask nonCurrentVersionCompletedTask = mock(StoreIngestionTask.class);
    doReturn(false).when(nonCurrentVersionCompletedTask).isCurrentVersion();
    doReturn(true).when(nonCurrentVersionCompletedTask).hasAllPartitionReportedCompleted();

    StoreIngestionTask currentVersionBootstrappingTask = mock(StoreIngestionTask.class);
    doReturn(true).when(currentVersionBootstrappingTask).isCurrentVersion();
    doReturn(false).when(currentVersionBootstrappingTask).hasAllPartitionReportedCompleted();

    StoreIngestionTask currentVersionCompletedTask = mock(StoreIngestionTask.class);
    doReturn(true).when(currentVersionCompletedTask).isCurrentVersion();
    doReturn(true).when(currentVersionCompletedTask).hasAllPartitionReportedCompleted();

    Map<String, StoreIngestionTask> mapContainsAllCompletedTask = new HashMap<>();
    mapContainsAllCompletedTask.put("non_current_version_completed", nonCurrentVersionCompletedTask);
    mapContainsAllCompletedTask.put("current_version_completed", currentVersionCompletedTask);

    assertFalse(KafkaStoreIngestionService.hasCurrentVersionBootstrapping(mapContainsAllCompletedTask));

    Map<String, StoreIngestionTask> mapContainsNonCurrentBootstrappingTask = new HashMap<>();
    mapContainsNonCurrentBootstrappingTask.put("non_current_version_bootstrapping", nonCurrentVersionBootstrappingTask);
    mapContainsNonCurrentBootstrappingTask.put("current_version_completed", currentVersionCompletedTask);

    assertFalse(KafkaStoreIngestionService.hasCurrentVersionBootstrapping(mapContainsNonCurrentBootstrappingTask));

    Map<String, StoreIngestionTask> mapContainsCurrentBootstrappingTask = new HashMap<>();
    mapContainsCurrentBootstrappingTask.put("non_current_version_bootstrapping", nonCurrentVersionBootstrappingTask);
    mapContainsCurrentBootstrappingTask.put("current_version_bootstrapping", currentVersionBootstrappingTask);

    assertTrue(KafkaStoreIngestionService.hasCurrentVersionBootstrapping(mapContainsCurrentBootstrappingTask));
  }

  @Test
  public void testDropStoragePartitionGracefully() throws NoSuchFieldException, IllegalAccessException {
    kafkaStoreIngestionService = mock(KafkaStoreIngestionService.class);
    String topicName = "test-store_v1";
    int partitionId = 0;
    VeniceProperties veniceProperties = AbstractStorageEngineTest.getServerProperties(PersistenceType.ROCKS_DB);
    VeniceStoreVersionConfig config = new VeniceStoreVersionConfig(topicName, veniceProperties);
    doCallRealMethod().when(kafkaStoreIngestionService).dropStoragePartitionGracefully(config, partitionId);

    Field topicLockManagerField = kafkaStoreIngestionService.getClass().getDeclaredField("topicLockManager");
    topicLockManagerField.setAccessible(true);
    topicLockManagerField.set(kafkaStoreIngestionService, new ResourceAutoClosableLockManager<>(ReentrantLock::new));

    NavigableMap topicNameToIngestionTaskMap = mock(NavigableMap.class);
    Field topicNameToIngestionTaskMapField =
        kafkaStoreIngestionService.getClass().getDeclaredField("topicNameToIngestionTaskMap");
    topicNameToIngestionTaskMapField.setAccessible(true);
    topicNameToIngestionTaskMapField.set(kafkaStoreIngestionService, topicNameToIngestionTaskMap);

    PubSubTopicRepository pubSubTopicRepository = mock(PubSubTopicRepository.class);
    Field pubSubTopicRepositoryField = kafkaStoreIngestionService.getClass().getDeclaredField("pubSubTopicRepository");
    pubSubTopicRepositoryField.setAccessible(true);
    pubSubTopicRepositoryField.set(kafkaStoreIngestionService, pubSubTopicRepository);

    StoreIngestionTask storeIngestionTask = mock(StoreIngestionTask.class);

    PriorityBlockingQueue consumerActionsQueue = mock(PriorityBlockingQueue.class);
    Field consumerActionsQueueField = StoreIngestionTask.class.getDeclaredField("consumerActionsQueue");
    consumerActionsQueueField.setAccessible(true);
    consumerActionsQueueField.set(storeIngestionTask, consumerActionsQueue);

    when(topicNameToIngestionTaskMap.get(topicName)).thenReturn(storeIngestionTask);
    doCallRealMethod().when(storeIngestionTask).dropStoragePartitionGracefully(any());

    PubSubTopic pubSubTopic = mock(PubSubTopic.class);
    when(pubSubTopicRepository.getTopic(topicName)).thenReturn(pubSubTopic);

    StorageService storageService = mock(StorageService.class);
    Field storageServiceField = StoreIngestionTask.class.getDeclaredField("storageService");
    storageServiceField.setAccessible(true);
    storageServiceField.set(storeIngestionTask, storageService);

    Field storeConfigField = StoreIngestionTask.class.getDeclaredField("storeVersionConfig");
    storeConfigField.setAccessible(true);
    storeConfigField.set(storeIngestionTask, config);

    // Verify that when the ingestion task is running, it drops the store partition asynchronously
    when(storeIngestionTask.isRunning()).thenReturn(true);
    kafkaStoreIngestionService.dropStoragePartitionGracefully(config, partitionId);
    verify(storeIngestionTask).dropStoragePartitionGracefully(any());
    verify(consumerActionsQueue).add(any());

    // Verify that when the ingestion task isn't running, it drops the store partition synchronously
    when(storeIngestionTask.isRunning()).thenReturn(false);
    kafkaStoreIngestionService.dropStoragePartitionGracefully(config, partitionId);
    verify(storageService).dropStorePartition(config, partitionId, true);
    kafkaStoreIngestionService.close();
  }

  @Test
  public void testCentralizedIdleIngestionTaskCleanupService() {
    kafkaStoreIngestionService = new KafkaStoreIngestionService(
        mockStorageService,
        mockVeniceConfigLoader,
        storageMetadataService,
        mockClusterInfoProvider,
        mockMetadataRepo,
        mockSchemaRepo,
        mockLiveClusterConfigRepo,
        new MetricsRepository(),
        Optional.empty(),
        Optional.empty(),
        AvroProtocolDefinition.PARTITION_STATE.getSerializer(),
        Optional.empty(),
        null,
        false,
        compressorFactory,
        Optional.empty(),
        null,
        false,
        null,
        mockPubSubClientsFactory,
        Optional.empty(),
        null,
        null,
        null);
    kafkaStoreIngestionService.start();
    String topicName = "test-store_v1";
    String storeName = Version.parseStoreFromKafkaTopicName(topicName);
    Store mockStore = new ZKStore(
        storeName,
        "unit-test",
        0,
        PersistenceType.ROCKS_DB,
        RoutingStrategy.CONSISTENT_HASH,
        ReadStrategy.ANY_OF_ONLINE,
        OfflinePushStrategy.WAIT_ALL_REPLICAS,
        1);

    mockStore.addVersion(new VersionImpl(storeName, 1, "test-job-id"));
    doReturn(mockStore).when(mockMetadataRepo).getStore(storeName);
    doReturn(mockStore).when(mockMetadataRepo).getStoreOrThrow(storeName);
    doReturn(new StoreVersionInfo(mockStore, mockStore.getVersion(1))).when(mockMetadataRepo)
        .waitVersion(eq(storeName), eq(1), any());
    VeniceProperties veniceProperties = AbstractStorageEngineTest.getServerProperties(PersistenceType.ROCKS_DB);
    VeniceStoreVersionConfig config = new VeniceStoreVersionConfig(topicName, veniceProperties);
    kafkaStoreIngestionService.startConsumption(config, 0);
    // kafkaStoreIngestionService.stopConsumptionAndWait(config, 0, 1, 1, true);
    final StoreIngestionTask storeIngestionTask = kafkaStoreIngestionService.getStoreIngestionTask(topicName);
    // Unsubscribe from partition 0 to make the store ingestion task idle
    Set<PubSubTopicPartition> topicPartitionsToUnsubscribe = new HashSet<>();
    topicPartitionsToUnsubscribe.add(new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic(topicName), 0));
    storeIngestionTask.consumerBatchUnsubscribe(topicPartitionsToUnsubscribe);
    // Verify that the store ingestion task is marked as idle and eventually closed
    TestUtils.waitForNonDeterministicAssertion(1, TimeUnit.MINUTES, () -> {
      Assert.assertTrue(storeIngestionTask.isIdleOverThreshold());
      Assert.assertNull(kafkaStoreIngestionService.getStoreIngestionTask(topicName));
    });
    kafkaStoreIngestionService.close();
  }

  @Test
  public void testInitParticipantConsumptionTask() {
    VeniceServerConfig mockServerConfig = mock(VeniceServerConfig.class);
    mockVeniceConfigLoader = mock(VeniceConfigLoader.class);
    mockClusterInfoProvider = mock(ClusterInfoProvider.class);
    ICProvider mockIcProvider = mock(ICProvider.class);
    ClientConfig mockClientConfig = mock(ClientConfig.class);

    when(mockServerConfig.isParticipantMessageStoreEnabled()).thenReturn(true);
    when(mockServerConfig.getClusterName()).thenReturn("testCluster");
    KafkaStoreIngestionService mockService = mock(KafkaStoreIngestionService.class);
    doCallRealMethod().when(mockService).initializeParticipantStoreConsumptionTask(any(), any(), any(), any(), any());

    ParticipantStoreConsumptionTask pct = mockService.initializeParticipantStoreConsumptionTask(
        mockServerConfig,
        Optional.of(mockClientConfig),
        mockClusterInfoProvider,
        new MetricsRepository(),
        mockIcProvider);
    assertNotNull(
        pct,
        "Participant consumption task should be initialized when participant message store is enabled and client config is present");

    // Case 2: Participant consumption task should not be initialized when participant message store is disabled
    when(mockServerConfig.isParticipantMessageStoreEnabled()).thenReturn(false);
    pct = mockService.initializeParticipantStoreConsumptionTask(
        mockServerConfig,
        Optional.of(mockClientConfig),
        mockClusterInfoProvider,
        new MetricsRepository(),
        mockIcProvider);
    assertNull(
        pct,
        "Participant consumption task should not be initialized when participant message store is disabled");

    // Case 3: Participant consumption task should not be initialized when client config is not present
    when(mockServerConfig.isParticipantMessageStoreEnabled()).thenReturn(true);
    pct = mockService.initializeParticipantStoreConsumptionTask(
        mockServerConfig,
        Optional.empty(),
        mockClusterInfoProvider,
        new MetricsRepository(),
        mockIcProvider);
    assertNull(pct, "Participant consumption task should not be initialized when client config is not present");
  }
}
