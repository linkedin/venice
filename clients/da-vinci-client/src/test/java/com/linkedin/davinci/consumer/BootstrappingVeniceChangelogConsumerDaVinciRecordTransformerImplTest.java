package com.linkedin.davinci.consumer;

import static com.linkedin.venice.ConfigKeys.CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.ZOOKEEPER_ADDRESS;
import static com.linkedin.venice.client.store.ClientConfig.DEFAULT_CLUSTER_DISCOVERY_D2_SERVICE_NAME;
import static com.linkedin.venice.offsets.OffsetRecord.LOWEST_OFFSET;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.ImmutableSet;
import com.linkedin.d2.balancer.D2Client;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciRecordTransformer;
import com.linkedin.davinci.client.DaVinciRecordTransformerConfig;
import com.linkedin.davinci.client.factory.CachingDaVinciClientFactory;
import com.linkedin.davinci.repository.NativeMetadataRepositoryViewAdapter;
import com.linkedin.venice.controllerapi.D2ControllerClient;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.serialization.avro.KafkaValueSerializer;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.utils.lazy.Lazy;
import com.linkedin.venice.views.ChangeCaptureView;
import io.tehuti.metrics.MetricsRepository;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.avro.Schema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class BootstrappingVeniceChangelogConsumerDaVinciRecordTransformerImplTest {
  private static final String TEST_CLUSTER_NAME = "test_cluster";
  private static final String TEST_ZOOKEEPER_ADDRESS = "test_zookeeper";
  public static final String D2_SERVICE_NAME = "ChildController";
  private static final String TEST_BOOTSTRAP_FILE_SYSTEM_PATH = "/export/content/data/change-capture";
  private static final long TEST_ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES = 1024L;
  private static final long TEST_DB_SYNC_BYTES_INTERVAL = 1000L;
  private static final int PARTITION_COUNT = 3;
  private static final int POLL_TIMEOUT = 1;
  private static final int CURRENT_STORE_VERSION = 1;

  private SchemaReader schemaReader;
  private PubSubConsumerAdapter pubSubConsumer;
  private String storeName;
  private String localStateTopicName;
  private RecordSerializer<String> keySerializer;
  private RecordSerializer<String> valueSerializer;
  private NativeMetadataRepositoryViewAdapter metadataRepository;
  private PubSubTopic changeCaptureTopic;
  private Schema keySchema;
  private Schema valueSchema;
  private final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
  private BootstrappingVeniceChangelogConsumerDaVinciRecordTransformerImpl<Integer, Integer> bootstrappingVeniceChangelogConsumer;
  private BootstrappingVeniceChangelogConsumerDaVinciRecordTransformerImpl.DaVinciRecordTransformerBootstrappingChangelogConsumer recordTransformer;
  private DaVinciRecordTransformerConfig mockDaVinciRecordTransformerConfig;
  private DaVinciClient mockDaVinciClient;

  @BeforeMethod
  public void setUp() throws NoSuchFieldException, IllegalAccessException {
    schemaReader = mock(SchemaReader.class);
    pubSubConsumer = mock(PubSubConsumerAdapter.class);
    storeName = "test-store";
    localStateTopicName = storeName + "-changeCaptureView" + "_Bootstrap_v1";
    schemaReader = mock(SchemaReader.class);
    keySchema = Schema.create(Schema.Type.INT);
    doReturn(keySchema).when(schemaReader).getKeySchema();
    valueSchema = Schema.create(Schema.Type.INT);
    doReturn(valueSchema).when(schemaReader).getValueSchema(1);

    keySerializer = FastSerializerDeserializerFactory.getFastAvroGenericSerializer(keySchema);
    valueSerializer = FastSerializerDeserializerFactory.getFastAvroGenericSerializer(valueSchema);

    D2ControllerClient d2ControllerClient = mock(D2ControllerClient.class);
    StoreResponse storeResponse = mock(StoreResponse.class);
    StoreInfo storeInfo = mock(StoreInfo.class);
    doReturn(1).when(storeInfo).getCurrentVersion();
    doReturn(2).when(storeInfo).getPartitionCount();
    doReturn(storeInfo).when(storeResponse).getStore();
    doReturn(storeResponse).when(d2ControllerClient).getStore(storeName);

    pubSubConsumer = mock(PubSubConsumerAdapter.class);

    PubSubTopic versionTopic = pubSubTopicRepository.getTopic(Version.composeKafkaTopic(storeName, 1));
    changeCaptureTopic =
        pubSubTopicRepository.getTopic(versionTopic.getName() + ChangeCaptureView.CHANGE_CAPTURE_TOPIC_SUFFIX);
    PubSubTopicPartition topicPartition_0 = new PubSubTopicPartitionImpl(changeCaptureTopic, 0);
    PubSubTopicPartition topicPartition_1 = new PubSubTopicPartitionImpl(changeCaptureTopic, 1);
    Set<PubSubTopicPartition> assignments = ImmutableSet.of(topicPartition_0, topicPartition_1);
    pubSubConsumer = mock(PubSubConsumerAdapter.class);
    doReturn(assignments).when(pubSubConsumer).getAssignment();
    doReturn(LOWEST_OFFSET).when(pubSubConsumer).getLatestOffset(topicPartition_0);
    doReturn(LOWEST_OFFSET).when(pubSubConsumer).getLatestOffset(topicPartition_1);
    doReturn(0L).when(pubSubConsumer).endOffset(topicPartition_0);
    doReturn(0L).when(pubSubConsumer).endOffset(topicPartition_1);
    when(pubSubConsumer.poll(anyLong())).thenReturn(new HashMap<>());

    Properties consumerProperties = new Properties();
    String localKafkaUrl = "http://www.fooAddress.linkedin.com:16337";
    consumerProperties.put(KAFKA_BOOTSTRAP_SERVERS, localKafkaUrl);
    consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaKeySerializer.class);
    consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaValueSerializer.class);
    consumerProperties.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 1024 * 1024);
    consumerProperties.put(CLUSTER_NAME, TEST_CLUSTER_NAME);
    consumerProperties.put(ZOOKEEPER_ADDRESS, TEST_ZOOKEEPER_ADDRESS);

    ChangelogClientConfig changelogClientConfig =
        new ChangelogClientConfig<>().setD2ControllerClient(d2ControllerClient)
            .setSchemaReader(schemaReader)
            .setStoreName(storeName)
            .setViewName("changeCaptureView")
            .setBootstrapFileSystemPath(TEST_BOOTSTRAP_FILE_SYSTEM_PATH)
            .setControllerD2ServiceName(D2_SERVICE_NAME)
            .setD2ServiceName(DEFAULT_CLUSTER_DISCOVERY_D2_SERVICE_NAME)
            .setConsumerProperties(consumerProperties)
            .setLocalD2ZkHosts(TEST_ZOOKEEPER_ADDRESS)
            .setRocksDBBlockCacheSizeInBytes(TEST_ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES)
            .setDatabaseSyncBytesInterval(TEST_DB_SYNC_BYTES_INTERVAL)
            .setIsBeforeImageView(true)
            .setD2Client(mock(D2Client.class))
            .setIsBlobTransferClientEnabled(true);
    changelogClientConfig.getInnerClientConfig().setMetricsRepository(new MetricsRepository());

    bootstrappingVeniceChangelogConsumer = new BootstrappingVeniceChangelogConsumerDaVinciRecordTransformerImpl<>(
        changelogClientConfig,
        pubSubConsumer,
        "");

    mockDaVinciRecordTransformerConfig = mock(DaVinciRecordTransformerConfig.class);
    recordTransformer = bootstrappingVeniceChangelogConsumer.new DaVinciRecordTransformerBootstrappingChangelogConsumer(
        CURRENT_STORE_VERSION, keySchema, valueSchema, valueSchema, mockDaVinciRecordTransformerConfig);

    // Replace daVinciClient with a mock
    mockDaVinciClient = mock(DaVinciClient.class);
    when(mockDaVinciClient.getPartitionCount()).thenReturn(PARTITION_COUNT);
    when(mockDaVinciClient.subscribe(any())).thenReturn(mock(CompletableFuture.class));

    Field daVinciClientField =
        BootstrappingVeniceChangelogConsumerDaVinciRecordTransformerImpl.class.getDeclaredField("daVinciClient");
    daVinciClientField.setAccessible(true);
    daVinciClientField.set(bootstrappingVeniceChangelogConsumer, mockDaVinciClient);
  }

  @Test
  public void testStartAllPartitions() throws IllegalAccessException, NoSuchFieldException {
    bootstrappingVeniceChangelogConsumer.start();

    // isStarted should be true
    Field isStartedField =
        BootstrappingVeniceChangelogConsumerDaVinciRecordTransformerImpl.class.getDeclaredField("isStarted");
    isStartedField.setAccessible(true);
    assertTrue((Boolean) isStartedField.get(bootstrappingVeniceChangelogConsumer));

    verify(mockDaVinciClient).start();

    Set<Integer> partitionSet = new HashSet<>();
    for (int i = 0; i < PARTITION_COUNT; i++) {
      partitionSet.add(i);
    }

    Field subscribedPartitionsField =
        BootstrappingVeniceChangelogConsumerDaVinciRecordTransformerImpl.class.getDeclaredField("subscribedPartitions");
    subscribedPartitionsField.setAccessible(true);
    assertEquals(subscribedPartitionsField.get(bootstrappingVeniceChangelogConsumer), partitionSet);
  }

  @Test
  public void testStartSpecificPartitions() throws IllegalAccessException, NoSuchFieldException {
    Set<Integer> partitionSet = Collections.singleton(1);
    bootstrappingVeniceChangelogConsumer.start(partitionSet);

    Field isStartedField =
        BootstrappingVeniceChangelogConsumerDaVinciRecordTransformerImpl.class.getDeclaredField("isStarted");
    isStartedField.setAccessible(true);
    assertTrue((Boolean) isStartedField.get(bootstrappingVeniceChangelogConsumer), "isStarted should be true");

    verify(mockDaVinciClient).start();

    Field subscribedPartitionsField =
        BootstrappingVeniceChangelogConsumerDaVinciRecordTransformerImpl.class.getDeclaredField("subscribedPartitions");
    subscribedPartitionsField.setAccessible(true);
    assertEquals(subscribedPartitionsField.get(bootstrappingVeniceChangelogConsumer), partitionSet);
  }

  @Test
  public void testStop() throws Exception {
    CachingDaVinciClientFactory daVinciClientFactoryMock = mock(CachingDaVinciClientFactory.class);
    Field daVinciClientFactoryField =
        BootstrappingVeniceChangelogConsumerDaVinciRecordTransformerImpl.class.getDeclaredField("daVinciClientFactory");
    daVinciClientFactoryField.setAccessible(true);
    daVinciClientFactoryField.set(bootstrappingVeniceChangelogConsumer, daVinciClientFactoryMock);

    bootstrappingVeniceChangelogConsumer.start();

    Field isStartedField =
        BootstrappingVeniceChangelogConsumerDaVinciRecordTransformerImpl.class.getDeclaredField("isStarted");
    isStartedField.setAccessible(true);
    assertTrue((Boolean) isStartedField.get(bootstrappingVeniceChangelogConsumer), "isStarted should be true");

    bootstrappingVeniceChangelogConsumer.stop();
    // isStarted should be false
    assertFalse((Boolean) isStartedField.get(bootstrappingVeniceChangelogConsumer));

    verify(daVinciClientFactoryMock).close();
  }

  @Test
  public void testPutAndDelete() {
    bootstrappingVeniceChangelogConsumer.start();
    recordTransformer.onStartVersionIngestion(true);

    int key = 1;
    int value = 2;
    Lazy<Integer> lazyKey = Lazy.of(() -> key);
    Lazy<Integer> lazyValue = Lazy.of(() -> value);

    for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
      recordTransformer.processPut(lazyKey, lazyValue, partitionId);
    }

    // Verify puts
    Collection<PubSubMessage<Integer, ChangeEvent<Integer>, VeniceChangeCoordinate>> pubSubMessages =
        bootstrappingVeniceChangelogConsumer.poll(POLL_TIMEOUT);
    assertEquals(pubSubMessages.size(), PARTITION_COUNT);
    for (PubSubMessage<Integer, ChangeEvent<Integer>, VeniceChangeCoordinate> message: pubSubMessages) {
      assertEquals((int) message.getKey(), key);
      ChangeEvent<Integer> changeEvent = message.getValue();
      assertNull(changeEvent.getPreviousValue());
      assertEquals((int) changeEvent.getCurrentValue(), value);
    }
    assertEquals(bootstrappingVeniceChangelogConsumer.poll(POLL_TIMEOUT).size(), 0, "Buffer should be empty");

    // Verify deletes
    for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
      recordTransformer.processDelete(lazyKey, partitionId);
    }
    pubSubMessages = bootstrappingVeniceChangelogConsumer.poll(POLL_TIMEOUT);
    assertEquals(pubSubMessages.size(), PARTITION_COUNT);
    for (PubSubMessage<Integer, ChangeEvent<Integer>, VeniceChangeCoordinate> message: pubSubMessages) {
      assertEquals((int) message.getKey(), key);
      ChangeEvent<Integer> changeEvent = message.getValue();
      assertNull(changeEvent.getPreviousValue());
      assertNull(changeEvent.getCurrentValue());
    }
    assertEquals(bootstrappingVeniceChangelogConsumer.poll(POLL_TIMEOUT).size(), 0, "Buffer should be empty");
  }

  @Test
  public void testOnlyCurrentVersionCanServe() {
    DaVinciRecordTransformer futureRecordTransformer =
        bootstrappingVeniceChangelogConsumer.new DaVinciRecordTransformerBootstrappingChangelogConsumer(
            CURRENT_STORE_VERSION + 1, keySchema, valueSchema, valueSchema, mockDaVinciRecordTransformerConfig);

    bootstrappingVeniceChangelogConsumer.start();
    recordTransformer.onStartVersionIngestion(true);
    futureRecordTransformer.onStartVersionIngestion(false);

    int key = 1;
    int currentVersionValue = 2;
    int futureVersionValue = 3;
    Lazy<Integer> lazyKey = Lazy.of(() -> key);
    Lazy<Integer> lazyCurrentVersionValueValue = Lazy.of(() -> currentVersionValue);
    Lazy<Integer> lazyFutureVersionValueValue = Lazy.of(() -> futureVersionValue);

    for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
      recordTransformer.processPut(lazyKey, lazyCurrentVersionValueValue, partitionId);
      futureRecordTransformer.processPut(lazyKey, lazyFutureVersionValueValue, partitionId);
    }

    // Verify it only contains current version values
    Collection<PubSubMessage<Integer, ChangeEvent<Integer>, VeniceChangeCoordinate>> pubSubMessages =
        bootstrappingVeniceChangelogConsumer.poll(POLL_TIMEOUT);
    assertEquals(pubSubMessages.size(), PARTITION_COUNT);
    for (PubSubMessage<Integer, ChangeEvent<Integer>, VeniceChangeCoordinate> message: pubSubMessages) {
      assertEquals((int) message.getKey(), key);
      ChangeEvent<Integer> changeEvent = message.getValue();
      assertNull(changeEvent.getPreviousValue());
      assertEquals((int) changeEvent.getCurrentValue(), currentVersionValue);
    }
    assertEquals(bootstrappingVeniceChangelogConsumer.poll(POLL_TIMEOUT).size(), 0, "Buffer should be empty");
  }
}
