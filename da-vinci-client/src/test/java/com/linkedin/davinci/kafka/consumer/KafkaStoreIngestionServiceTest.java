package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.compression.StorageEngineBackedCompressorFactory;
import com.linkedin.davinci.config.VeniceClusterConfig;
import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.storage.StorageEngineRepository;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.store.AbstractStorageEngineTest;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.ClusterInfoProvider;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.ReadStrategy;
import com.linkedin.venice.meta.RoutingStrategy;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.meta.ZKStore;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.linkedin.venice.ConfigKeys.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class KafkaStoreIngestionServiceTest {
  private StorageEngineRepository mockStorageEngineRepository;
  private VeniceConfigLoader mockVeniceConfigLoader;
  private StorageMetadataService storageMetadataService;
  private ClusterInfoProvider mockClusterInfoProvider;
  private ReadOnlyStoreRepository mockMetadataRepo;
  private ReadOnlySchemaRepository mockSchemaRepo;
  private StorageEngineBackedCompressorFactory compressorFactory;

  private KafkaStoreIngestionService kafkaStoreIngestionService;

  @BeforeClass
  public void setup() {
    mockStorageEngineRepository = mock(StorageEngineRepository.class);
    storageMetadataService = mock(StorageMetadataService.class);
    mockClusterInfoProvider = mock(ClusterInfoProvider.class);
    mockMetadataRepo = mock(ReadOnlyStoreRepository.class);
    mockSchemaRepo = mock(ReadOnlySchemaRepository.class);
    compressorFactory = new StorageEngineBackedCompressorFactory(storageMetadataService);

    setupMockConfig();
  }

  private void setupMockConfig() {
    mockVeniceConfigLoader = mock(VeniceConfigLoader.class);

    VeniceServerConfig mockVeniceServerConfig = mock(VeniceServerConfig.class);
    doReturn(-1l).when(mockVeniceServerConfig).getKafkaFetchQuotaBytesPerSecond();
    doReturn(-1l).when(mockVeniceServerConfig).getKafkaFetchQuotaRecordPerSecond();
    doReturn(-1l).when(mockVeniceServerConfig).getKafkaFetchQuotaUnorderedBytesPerSecond();
    doReturn(-1l).when(mockVeniceServerConfig).getKafkaFetchQuotaUnorderedRecordPerSecond();
    doReturn("").when(mockVeniceServerConfig).getDataBasePath();
    doReturn(0.9d).when(mockVeniceServerConfig).getDiskFullThreshold();

    VeniceClusterConfig mockVeniceClusterConfig = mock(VeniceClusterConfig.class);
    doReturn("localhost:1234").when(mockVeniceClusterConfig).getKafkaZkAddress();
    Properties properties = new Properties();
    properties.put(KAFKA_ZK_ADDRESS, "localhost:1234");
    properties.put(KAFKA_BOOTSTRAP_SERVERS, "localhost:16637");
    VeniceProperties mockVeniceProperties = new VeniceProperties(properties);
    doReturn(mockVeniceProperties).when(mockVeniceClusterConfig).getClusterProperties();

    doReturn(mockVeniceServerConfig).when(mockVeniceConfigLoader).getVeniceServerConfig();
    doReturn(mockVeniceClusterConfig).when(mockVeniceConfigLoader).getVeniceClusterConfig();
  }


 @Test
 public void testDisableMetricsEmission() {
   kafkaStoreIngestionService = new KafkaStoreIngestionService(
       mockStorageEngineRepository,
       mockVeniceConfigLoader,
       storageMetadataService, mockClusterInfoProvider, mockMetadataRepo,
       mockSchemaRepo,
       new MetricsRepository(),
       null,
       Optional.empty(),
       Optional.empty(),
       AvroProtocolDefinition.PARTITION_STATE.getSerializer(),
       Optional.empty(),
       null,
       false,
       compressorFactory,
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

   for (int i = 1; i <= taskNum; i ++ ) {
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


   String mostRecentTopic = mockStoreName + "_v" +taskNum;
   topicNameToIngestionTaskMap.forEach((topicName, task) -> {
     if (Version.parseStoreFromKafkaTopicName(topicName).equals(mockStoreName)) {
       if (topicName.equals(mostRecentTopic)) {
         verify(task).enableMetricsEmission();
       } else {
         verify(task).disableMetricsEmission();
       }
     } else { //checks store with similar name will not be call
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
        storageMetadataService, mockClusterInfoProvider, mockMetadataRepo,
        mockSchemaRepo,
        new MetricsRepository(),
        null,
        Optional.empty(),
        Optional.empty(),
        AvroProtocolDefinition.PARTITION_STATE.getSerializer(),
        Optional.empty(),
        null,
        false,
        compressorFactory,
        Optional.empty());
    String topic1 = "test-store_v1";
    String topic2 = "test-store_v2";
    String invalidTopic = "invalid-store_v1";
    String storeName = Version.parseStoreFromKafkaTopicName(topic1);
    String deletedStoreName = Version.parseStoreFromKafkaTopicName(invalidTopic);
    Store mockStore = new ZKStore(storeName, "unit-test", 0, PersistenceType.ROCKS_DB,
        RoutingStrategy.CONSISTENT_HASH, ReadStrategy.ANY_OF_ONLINE, OfflinePushStrategy.WAIT_ALL_REPLICAS, 1);
    Store toBeDeletedStore = new ZKStore(deletedStoreName, "unit-test", 0, PersistenceType.ROCKS_DB,
        RoutingStrategy.CONSISTENT_HASH, ReadStrategy.ANY_OF_ONLINE, OfflinePushStrategy.WAIT_ALL_REPLICAS, 1);
    mockStore.addVersion(new VersionImpl(storeName, 1, "test-job-id"));
    toBeDeletedStore.addVersion(new VersionImpl(deletedStoreName, 1, "test-job-id"));
    doReturn(mockStore).when(mockMetadataRepo).getStore(storeName);
    doReturn(toBeDeletedStore).when(mockMetadataRepo).getStore(deletedStoreName);
    doReturn(mockStore).when(mockMetadataRepo).getStoreOrThrow(storeName);
    doReturn(toBeDeletedStore).when(mockMetadataRepo).getStoreOrThrow(deletedStoreName);
    doReturn(new Pair<>(mockStore, mockStore.getVersion(1).get())).when(mockMetadataRepo).waitVersion(eq(storeName), eq(1), any());
    doReturn(new Pair<>(toBeDeletedStore, toBeDeletedStore.getVersion(1).get())).when(mockMetadataRepo).waitVersion(eq(deletedStoreName), eq(1), any());
    VeniceProperties veniceProperties = AbstractStorageEngineTest.getServerProperties(PersistenceType.ROCKS_DB);
    kafkaStoreIngestionService.startConsumption(new VeniceStoreVersionConfig(topic1, veniceProperties), 0);
    assertEquals(kafkaStoreIngestionService.getIngestingTopicsWithVersionStatusNotOnline().size(), 1,
        "Unexpected number of ingesting topics with version status of not ONLINE");
    mockStore.updateVersionStatus(1, VersionStatus.ONLINE);
    assertEquals(kafkaStoreIngestionService.getIngestingTopicsWithVersionStatusNotOnline().size(), 0,
       "Expecting an empty set since all ingesting topics have version status of ONLINE");
    mockStore.addVersion(new VersionImpl(storeName, 2, "test-job-id"));
    doReturn(new Pair<>(mockStore, mockStore.getVersion(2).get())).when(mockMetadataRepo).waitVersion(eq(storeName), eq(2), any());
    kafkaStoreIngestionService.startConsumption(new VeniceStoreVersionConfig(topic2, veniceProperties), 0);
    kafkaStoreIngestionService.startConsumption(new VeniceStoreVersionConfig(invalidTopic, veniceProperties), 0);
    doThrow(new VeniceNoStoreException(deletedStoreName)).when(mockMetadataRepo).getStoreOrThrow(deletedStoreName);
    doReturn(null).when(mockMetadataRepo).getStore(deletedStoreName);
    assertEquals(kafkaStoreIngestionService.getIngestingTopicsWithVersionStatusNotOnline().size(), 2,
        "Invalid and in flight ingesting topics should be included in the returned set");
    mockStore.updateVersionStatus(2, VersionStatus.ONLINE);
    mockStore.deleteVersion(1);
    Set<String> results = kafkaStoreIngestionService.getIngestingTopicsWithVersionStatusNotOnline();
    assertTrue(results.size() == 2 && results.contains(invalidTopic) && results.contains(topic1),
        "Invalid and retired ingesting topics should be included in the returned set");
  }
}
