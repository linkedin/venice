package com.linkedin.davinci.kafka.consumer;

import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.compression.StorageEngineBackedCompressorFactory;
import com.linkedin.davinci.config.VeniceClusterConfig;
import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.listener.response.MetadataResponse;
import com.linkedin.davinci.listener.response.ServerCurrentVersionResponse;
import com.linkedin.davinci.storage.StorageEngineRepository;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.davinci.store.AbstractStorageEngineTest;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.helix.HelixCustomizedViewOfflinePushRepository;
import com.linkedin.venice.helix.HelixInstanceConfigRepository;
import com.linkedin.venice.meta.ClusterInfoProvider;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadOnlyLiveClusterConfigRepository;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.ReadStrategy;
import com.linkedin.venice.meta.RoutingStrategy;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.meta.ZKStore;
import com.linkedin.venice.metadata.response.VersionProperties;
import com.linkedin.venice.pubsub.PubSubAdminAdapterFactory;
import com.linkedin.venice.pubsub.PubSubClientsFactory;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterFactory;
import com.linkedin.venice.pubsub.PubSubProducerAdapterFactory;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import it.unimi.dsi.fastutil.objects.Object2IntMaps;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Function;
import org.apache.avro.Schema;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


@Test
public abstract class KafkaStoreIngestionServiceTest {
  private StorageEngineRepository mockStorageEngineRepository;
  private VeniceConfigLoader mockVeniceConfigLoader;
  private StorageMetadataService storageMetadataService;
  private ClusterInfoProvider mockClusterInfoProvider;
  private ReadOnlyStoreRepository mockMetadataRepo;
  private ReadOnlySchemaRepository mockSchemaRepo;
  private HelixCustomizedViewOfflinePushRepository mockCustomizedViewRepository;
  private HelixInstanceConfigRepository mockHelixInstanceConfigRepository;
  private ReadOnlyLiveClusterConfigRepository mockLiveClusterConfigRepo;
  private PubSubClientsFactory mockPubSubClientsFactory;
  private StorageEngineBackedCompressorFactory compressorFactory;

  private KafkaStoreIngestionService kafkaStoreIngestionService;

  @BeforeClass
  public void setUp() {
    mockStorageEngineRepository = mock(StorageEngineRepository.class);
    doReturn(mock(AbstractStorageEngine.class)).when(mockStorageEngineRepository).getLocalStorageEngine(anyString());
    storageMetadataService = mock(StorageMetadataService.class);
    mockClusterInfoProvider = mock(ClusterInfoProvider.class);
    mockMetadataRepo = mock(ReadOnlyStoreRepository.class);
    mockSchemaRepo = mock(ReadOnlySchemaRepository.class);
    mockCustomizedViewRepository = mock(HelixCustomizedViewOfflinePushRepository.class);
    mockHelixInstanceConfigRepository = mock(HelixInstanceConfigRepository.class);
    mockLiveClusterConfigRepo = mock(ReadOnlyLiveClusterConfigRepository.class);
    mockPubSubClientsFactory = new PubSubClientsFactory(
        mock(PubSubProducerAdapterFactory.class),
        mock(PubSubConsumerAdapterFactory.class),
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

    // Consumer related configs for preparing kafka consumer service.
    doReturn(dummyKafkaUrl).when(mockVeniceServerConfig).getKafkaBootstrapServers();
    Function<String, String> kafkaClusterUrlResolver = String::toString;
    doReturn(kafkaClusterUrlResolver).when(mockVeniceServerConfig).getKafkaClusterUrlResolver();
    doReturn(VeniceProperties.empty()).when(mockVeniceServerConfig).getKafkaConsumerConfigsForLocalConsumption();
    doReturn(getConsumerAssignmentStrategy()).when(mockVeniceServerConfig).getSharedConsumerAssignmentStrategy();
    doReturn(1).when(mockVeniceServerConfig).getConsumerPoolSizePerKafkaCluster();
    doReturn(SecurityProtocol.PLAINTEXT).when(mockVeniceServerConfig).getKafkaSecurityProtocol(dummyKafkaUrl);
    doReturn(10).when(mockVeniceServerConfig).getKafkaMaxPollRecords();
    doReturn(2).when(mockVeniceServerConfig).getTopicManagerMetadataFetcherConsumerPoolSize();
    doReturn(2).when(mockVeniceServerConfig).getTopicManagerMetadataFetcherThreadPoolSize();

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
        mockStorageEngineRepository,
        mockVeniceConfigLoader,
        storageMetadataService,
        mockClusterInfoProvider,
        mockMetadataRepo,
        mockSchemaRepo,
        Optional.empty(),
        Optional.empty(),
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
        Optional.empty());

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
  }

  @Test
  public void testGetIngestingTopicsNotWithOnlineVersion() {
    // Without starting the ingestion service test getIngestingTopicsWithVersionStatusNotOnline would return the correct
    // topics under different scenarios.
    kafkaStoreIngestionService = new KafkaStoreIngestionService(
        mockStorageEngineRepository,
        mockVeniceConfigLoader,
        storageMetadataService,
        mockClusterInfoProvider,
        mockMetadataRepo,
        mockSchemaRepo,
        Optional.empty(),
        Optional.empty(),
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
        Optional.empty());
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
    doReturn(new Pair<>(mockStore, mockStore.getVersion(1).get())).when(mockMetadataRepo)
        .waitVersion(eq(storeName), eq(1), any());
    doReturn(new Pair<>(toBeDeletedStore, toBeDeletedStore.getVersion(1).get())).when(mockMetadataRepo)
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
    doReturn(new Pair<>(mockStore, mockStore.getVersion(2).get())).when(mockMetadataRepo)
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
  }

  @Test
  public void testCloseStoreIngestionTask() {
    kafkaStoreIngestionService = new KafkaStoreIngestionService(
        mockStorageEngineRepository,
        mockVeniceConfigLoader,
        storageMetadataService,
        mockClusterInfoProvider,
        mockMetadataRepo,
        mockSchemaRepo,
        Optional.empty(),
        Optional.empty(),
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
        Optional.empty());
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

    AbstractStorageEngine storageEngine1 = mock(AbstractStorageEngine.class);
    Mockito.when(mockStorageEngineRepository.getLocalStorageEngine(topicName)).thenReturn(storageEngine1);

    mockStore.addVersion(new VersionImpl(storeName, 1, "test-job-id"));
    doReturn(mockStore).when(mockMetadataRepo).getStore(storeName);
    doReturn(mockStore).when(mockMetadataRepo).getStoreOrThrow(storeName);
    doReturn(new Pair<>(mockStore, mockStore.getVersion(1).get())).when(mockMetadataRepo)
        .waitVersion(eq(storeName), eq(1), any());
    VeniceProperties veniceProperties = AbstractStorageEngineTest.getServerProperties(PersistenceType.ROCKS_DB);
    kafkaStoreIngestionService.startConsumption(new VeniceStoreVersionConfig(topicName, veniceProperties), 0);
    StoreIngestionTask storeIngestionTask = kafkaStoreIngestionService.getStoreIngestionTask(topicName);
    kafkaStoreIngestionService.shutdownStoreIngestionTask(topicName);
    StoreIngestionTask closedStoreIngestionTask = kafkaStoreIngestionService.getStoreIngestionTask(topicName);
    Assert.assertNull(closedStoreIngestionTask);

    AbstractStorageEngine storageEngine2 = mock(AbstractStorageEngine.class);
    Mockito.when(mockStorageEngineRepository.getLocalStorageEngine(topicName)).thenReturn(storageEngine2);
    kafkaStoreIngestionService.startConsumption(new VeniceStoreVersionConfig(topicName, veniceProperties), 0);
    StoreIngestionTask newStoreIngestionTask = kafkaStoreIngestionService.getStoreIngestionTask(topicName);
    Assert.assertNotNull(newStoreIngestionTask);
    Assert.assertNotEquals(storeIngestionTask, newStoreIngestionTask);
    assertEquals(newStoreIngestionTask.getStorageEngine(), storageEngine2);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testStoreIngestionTaskShutdownLastPartition(boolean isIsolatedIngestion) {
    kafkaStoreIngestionService = new KafkaStoreIngestionService(
        mockStorageEngineRepository,
        mockVeniceConfigLoader,
        storageMetadataService,
        mockClusterInfoProvider,
        mockMetadataRepo,
        mockSchemaRepo,
        Optional.empty(),
        Optional.empty(),
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
        Optional.empty());
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

    AbstractStorageEngine storageEngine1 = mock(AbstractStorageEngine.class);
    Mockito.when(mockStorageEngineRepository.getLocalStorageEngine(topicName)).thenReturn(storageEngine1);

    mockStore.addVersion(new VersionImpl(storeName, 1, "test-job-id"));
    doReturn(mockStore).when(mockMetadataRepo).getStore(storeName);
    doReturn(mockStore).when(mockMetadataRepo).getStoreOrThrow(storeName);
    doReturn(new Pair<>(mockStore, mockStore.getVersion(1).get())).when(mockMetadataRepo)
        .waitVersion(eq(storeName), eq(1), any());
    VeniceProperties veniceProperties = AbstractStorageEngineTest.getServerProperties(PersistenceType.ROCKS_DB);
    VeniceStoreVersionConfig config = new VeniceStoreVersionConfig(topicName, veniceProperties);
    kafkaStoreIngestionService.startConsumption(config, 0);
    kafkaStoreIngestionService.stopConsumptionAndWait(config, 0, 1, 1, true);
    StoreIngestionTask storeIngestionTask = kafkaStoreIngestionService.getStoreIngestionTask(topicName);
    if (isIsolatedIngestion) {
      Assert.assertNotNull(storeIngestionTask);
    } else {
      Assert.assertNull(storeIngestionTask);
    }
    kafkaStoreIngestionService.startConsumption(config, 0);
    storeIngestionTask = kafkaStoreIngestionService.getStoreIngestionTask(topicName);
    storeIngestionTask.setPartitionConsumptionState(1, mock(PartitionConsumptionState.class));
    kafkaStoreIngestionService.stopConsumptionAndWait(config, 0, 1, 1, true);
    Assert.assertNotNull(storeIngestionTask);
  }

  @Test
  public void testGetMetadata() {
    MetricsRepository metricsRepository = new MetricsRepository();
    kafkaStoreIngestionService = new KafkaStoreIngestionService(
        mockStorageEngineRepository,
        mockVeniceConfigLoader,
        storageMetadataService,
        mockClusterInfoProvider,
        mockMetadataRepo,
        mockSchemaRepo,
        Optional.of(CompletableFuture.completedFuture(mockCustomizedViewRepository)),
        Optional.of(CompletableFuture.completedFuture(mockHelixInstanceConfigRepository)),
        mockLiveClusterConfigRepo,
        metricsRepository,
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
        Optional.empty());
    String storeName = "test-store";
    String otherStoreName = "test-store2";
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
    mockStore.addVersion(new VersionImpl(storeName, 2, "test-job-id2"));
    mockStore.setCurrentVersion(2);
    String topicName = Version.composeKafkaTopic(storeName, 2);
    PartitionAssignment partitionAssignment = new PartitionAssignment(topicName, 1);
    Partition partition = mock(Partition.class);
    doReturn(0).when(partition).getId();
    List<Instance> readyToServeInstances = Collections.singletonList(new Instance("host1", "host1", 1234));
    doReturn(readyToServeInstances).when(partition).getReadyToServeInstances();
    partitionAssignment.addPartition(partition);

    String schema = "\"string\"";
    doReturn(mockStore).when(mockMetadataRepo).getStoreOrThrow(storeName);
    Mockito.when(mockSchemaRepo.getKeySchema(storeName)).thenReturn(new SchemaEntry(0, schema));
    Mockito.when(mockSchemaRepo.getValueSchemas(storeName))
        .thenReturn(Collections.singletonList(new SchemaEntry(0, schema)));
    Mockito.when(mockCustomizedViewRepository.getPartitionAssignments(topicName)).thenReturn(partitionAssignment);
    Mockito.when(mockHelixInstanceConfigRepository.getInstanceGroupIdMapping()).thenReturn(Collections.emptyMap());

    MetadataResponse metadataResponse = kafkaStoreIngestionService.getMetadata(storeName);
    Assert.assertNotNull(metadataResponse);
    Assert.assertEquals(metadataResponse.getResponseRecord().getKeySchema().get("0"), "\"string\"");
    // Verify the metadata
    Assert.assertEquals(metadataResponse.getResponseRecord().getVersions().size(), 2);
    VersionProperties versionProperties = metadataResponse.getResponseRecord().getVersionMetadata();
    Assert.assertNotNull(versionProperties);
    Assert.assertEquals(versionProperties.getCurrentVersion(), 2);
    Assert.assertEquals(versionProperties.getPartitionCount(), 1);
    Assert.assertEquals(metadataResponse.getResponseRecord().getRoutingInfo().get("0").size(), 1);
    String metadataInvokeMetricName = "." + storeName + "--request_based_metadata_invoke_count.Rate";
    String metadataFailureMetricName = "." + storeName + "--request_based_metadata_failure_count.Rate";
    Assert.assertTrue(metricsRepository.getMetric(metadataInvokeMetricName).value() > 0);
    Assert.assertEquals(metricsRepository.getMetric(metadataFailureMetricName).value(), 0d);

    ServerCurrentVersionResponse currentVersionResponse =
        kafkaStoreIngestionService.getCurrentVersionResponse(storeName);
    Assert.assertNotNull(currentVersionResponse);
    Assert.assertEquals(currentVersionResponse.getCurrentVersion(), 2);

  }
}
