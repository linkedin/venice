package com.linkedin.davinci.consumer;

import static com.linkedin.venice.ConfigKeys.CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.ZOOKEEPER_ADDRESS;
import static com.linkedin.venice.offsets.OffsetRecord.LOWEST_OFFSET;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.davinci.repository.ThinClientMetaStoreBasedRepository;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.davinci.store.record.ValueRecord;
import com.linkedin.venice.client.change.capture.protocol.RecordChangeEvent;
import com.linkedin.venice.client.change.capture.protocol.ValueBytes;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.D2ControllerClient;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.pubsub.ImmutablePubSubMessage;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.serialization.avro.KafkaValueSerializer;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.views.ChangeCaptureView;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class InternalLocalBootstrappingVeniceChangelogConsumerTest {
  private static final String TEST_CLUSTER_NAME = "test_cluster";
  private static final String TEST_ZOOKEEPER_ADDRESS = "test_zookeeper";
  private static final String TEST_KEY_1 = "key_1";
  private static final String TEST_KEY_2 = "key_2";
  private static final String TEST_KEY_3 = "key_3";
  private static final String TEST_OLD_VALUE_1 = "old_value_1";
  private static final String TEST_NEW_VALUE_1 = "new_value_1";
  private static final String TEST_OLD_VALUE_2 = "old_value_2";
  private static final String TEST_NEW_VALUE_2 = "new_value_2";
  private static final String TEST_OLD_VALUE_3 = "old_value_3";
  private static final String TEST_NEW_VALUE_3 = "new_value_3";
  private static final String TEST_BOOTSTRAP_FILE_SYSTEM_PATH = "/export/content/data/change-capture";
  private static final int TEST_SCHEMA_ID = 1;
  private String storeName;
  private InternalLocalBootstrappingVeniceChangelogConsumer<Utf8, Utf8> bootstrappingVeniceChangelogConsumer;
  private RecordSerializer<String> keySerializer;
  private RecordSerializer<String> valueSerializer;
  private PubSubConsumerAdapter pubSubConsumer;
  private ThinClientMetaStoreBasedRepository metadataRepository;
  private PubSubTopic changeCaptureTopic;
  private SchemaReader schemaReader;
  private Schema valueSchema;
  private final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
  private static final Map<String, ChangeEvent<String>> TEST_RECORDS = ImmutableMap.of(
      TEST_KEY_1,
      new ChangeEvent<>(TEST_OLD_VALUE_1, TEST_NEW_VALUE_1),
      TEST_KEY_2,
      new ChangeEvent<>(TEST_OLD_VALUE_2, TEST_NEW_VALUE_2),
      TEST_KEY_3,
      new ChangeEvent<>(TEST_OLD_VALUE_3, TEST_NEW_VALUE_3));

  @BeforeMethod
  public void setUp() {
    storeName = Utils.getUniqueString();
    schemaReader = mock(SchemaReader.class);
    Schema keySchema = AvroCompatibilityHelper.parse("\"string\"");
    doReturn(keySchema).when(schemaReader).getKeySchema();
    valueSchema = AvroCompatibilityHelper.parse("\"string\"");
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
            .setConsumerProperties(consumerProperties)
            .setLocalD2ZkHosts(TEST_ZOOKEEPER_ADDRESS);
    bootstrappingVeniceChangelogConsumer =
        new InternalLocalBootstrappingVeniceChangelogConsumer<>(changelogClientConfig, pubSubConsumer);

    metadataRepository = mock(ThinClientMetaStoreBasedRepository.class);
    Store store = mock(Store.class);
    Version mockVersion = new VersionImpl(storeName, 1, "foo");
    when(store.getCurrentVersion()).thenReturn(1);
    when(store.getCompressionStrategy()).thenReturn(CompressionStrategy.NO_OP);
    when(metadataRepository.getStore(anyString())).thenReturn(store);
    when(store.getVersion(Mockito.anyInt())).thenReturn(Optional.of(mockVersion));
    when(metadataRepository.getValueSchema(storeName, TEST_SCHEMA_ID))
        .thenReturn(new SchemaEntry(TEST_SCHEMA_ID, valueSchema));
    bootstrappingVeniceChangelogConsumer.setStoreRepository(metadataRepository);
  }

  @Test
  public void testStart() throws ExecutionException, InterruptedException {
    PubSubTopic versionTopic = pubSubTopicRepository.getTopic(Version.composeKafkaTopic(storeName, 1));
    PubSubTopic changeCaptureTopic =
        pubSubTopicRepository.getTopic(versionTopic.getName() + ChangeCaptureView.CHANGE_CAPTURE_TOPIC_SUFFIX);
    PubSubTopicPartition topicPartition_0 = new PubSubTopicPartitionImpl(changeCaptureTopic, 0);
    PubSubTopicPartition topicPartition_1 = new PubSubTopicPartitionImpl(changeCaptureTopic, 1);
    Set<PubSubTopicPartition> assignments = ImmutableSet.of(topicPartition_0, topicPartition_1);
    doReturn(assignments).when(pubSubConsumer).getAssignment();
    doReturn(0L).when(pubSubConsumer).getLatestOffset(topicPartition_0);
    doReturn(0L).when(pubSubConsumer).getLatestOffset(topicPartition_1);
    doReturn(1L).when(pubSubConsumer).endOffset(topicPartition_0);
    doReturn(1L).when(pubSubConsumer).endOffset(topicPartition_1);
    when(pubSubConsumer.poll(anyLong())).thenReturn(new HashMap<>());

    StorageService mockStorageService = mock(StorageService.class);
    AbstractStorageEngine mockStorageEngine = mock(AbstractStorageEngine.class);
    when(mockStorageService.getStorageEngine(anyString())).thenReturn(mockStorageEngine);
    StorageMetadataService mockStorageMetadataService = mock(StorageMetadataService.class);
    when(mockStorageMetadataService.getLastOffset(anyString(), anyInt()))
        .thenReturn(new OffsetRecord(mock(InternalAvroSpecificSerializer.class)));
    bootstrappingVeniceChangelogConsumer.setStorageAndMetadataService(mockStorageService, mockStorageMetadataService);

    when(pubSubConsumer.poll(anyLong()))
        .thenReturn(prepareChangeCaptureRecordsToBePolled(TEST_KEY_1, changeCaptureTopic, 0))
        .thenReturn(prepareChangeCaptureRecordsToBePolled(TEST_KEY_2, changeCaptureTopic, 1));

    bootstrappingVeniceChangelogConsumer.start().get();

    // verify the consumer start
    verify(mockStorageService, times(1)).start();
    verify(mockStorageService, times(1)).openStoreForNewPartition(any(), eq(0), any());
    verify(mockStorageService, times(1)).openStoreForNewPartition(any(), eq(1), any());
    verify(metadataRepository, times(1)).start();
    verify(metadataRepository, times(1)).subscribe(storeName);
    verify(metadataRepository, times(1)).refresh();
    verify(pubSubConsumer, times(1)).subscribe(topicPartition_0, LOWEST_OFFSET);
    verify(pubSubConsumer, times(1)).subscribe(topicPartition_1, LOWEST_OFFSET);
    verify(pubSubConsumer, times(1)).subscribe(topicPartition_0, 0L);
    verify(pubSubConsumer, times(1)).subscribe(topicPartition_1, 0L);
    verify(pubSubConsumer, times(2)).poll(anyLong());

    // Verify onRecordReceivedForStorage for partition 0
    Collection<PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate>> resultSet = new ArrayList<>();
    Map<PubSubTopicPartition, List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> testRecords =
        prepareChangeCaptureRecordsToBePolled(TEST_KEY_1, changeCaptureTopic, 0);
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> testRecord =
        testRecords.values().stream().findFirst().get().get(0);
    Map<Integer, String> expectedPartitionToKey = new HashMap<>();
    expectedPartitionToKey.put(0, TEST_KEY_1);
    ValueBytes valueBytes = new ValueBytes();
    valueBytes.schemaId = TEST_SCHEMA_ID;
    valueBytes.value = ByteBuffer.wrap(valueSerializer.serialize(TEST_NEW_VALUE_1));
    bootstrappingVeniceChangelogConsumer.onRecordReceivedForStorage(
        testRecord.getKey().getKey(),
        ValueRecord.create(TEST_SCHEMA_ID, valueBytes.value.array()).serialize(),
        0,
        resultSet);

    verifyPollResult(resultSet, expectedPartitionToKey, true);

    // Verify onCompletionForStorage for partition 0
    resultSet = new ArrayList<>();
    InternalLocalBootstrappingVeniceChangelogConsumer.BootstrapState state =
        new InternalLocalBootstrappingVeniceChangelogConsumer.BootstrapState();
    AtomicBoolean completed = new AtomicBoolean(false);

    bootstrappingVeniceChangelogConsumer.onCompletionForStorage(0, state, resultSet, completed);

    Assert.assertEquals(resultSet.size(), 0);
    Assert.assertTrue(completed.get());
    Assert.assertEquals(state.bootstrapState, InternalLocalBootstrappingVeniceChangelogConsumer.PollState.CONSUMING);
    Assert.assertEquals(bootstrappingVeniceChangelogConsumer.getBootstrapCompletedCount(), 1);

    // Verify onRecordReceivedForStorage for partition 1
    resultSet = new ArrayList<>();
    testRecords = prepareChangeCaptureRecordsToBePolled(TEST_KEY_2, changeCaptureTopic, 1);
    testRecord = testRecords.values().stream().findFirst().get().get(0);
    expectedPartitionToKey = new HashMap<>();
    expectedPartitionToKey.put(1, TEST_KEY_2);
    valueBytes = new ValueBytes();
    valueBytes.schemaId = TEST_SCHEMA_ID;
    valueBytes.value = ByteBuffer.wrap(valueSerializer.serialize(TEST_NEW_VALUE_2));

    bootstrappingVeniceChangelogConsumer.onRecordReceivedForStorage(
        testRecord.getKey().getKey(),
        ValueRecord.create(TEST_SCHEMA_ID, valueBytes.value.array()).serialize(),
        1,
        resultSet);

    verifyPollResult(resultSet, expectedPartitionToKey, true);

    // Verify onCompletionForStorage for partition 1
    resultSet = new ArrayList<>();
    state = new InternalLocalBootstrappingVeniceChangelogConsumer.BootstrapState();
    completed = new AtomicBoolean(false);

    bootstrappingVeniceChangelogConsumer.onCompletionForStorage(1, state, resultSet, completed);

    Assert.assertEquals(resultSet.size(), 1);
    Assert.assertTrue(completed.get());
    Assert.assertEquals(state.bootstrapState, InternalLocalBootstrappingVeniceChangelogConsumer.PollState.CONSUMING);
    Assert.assertEquals(bootstrappingVeniceChangelogConsumer.getBootstrapCompletedCount(), 2);
  }

  private void verifyPollResult(
      Collection<PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate>> bootstrapResult,
      Map<Integer, String> expectedPartitionToKey,
      boolean isBootstrap) {
    Assert.assertEquals(1, bootstrapResult.size());
    PubSubMessage<Utf8, ChangeEvent<Utf8>, VeniceChangeCoordinate> record = bootstrapResult.stream().findFirst().get();
    String expectedKey = expectedPartitionToKey.get(record.getPartition());
    ChangeEvent<String> value = TEST_RECORDS.get(expectedKey);
    Assert.assertNotNull(value);
    Assert.assertEquals(expectedKey, record.getKey().toString());
    if (isBootstrap) {
      Assert.assertNull(record.getValue().getPreviousValue());
    } else {

      Assert.assertEquals(value.getPreviousValue(), record.getValue().getPreviousValue().toString());
    }

    Assert.assertEquals(value.getCurrentValue(), record.getValue().getCurrentValue().toString());
  }

  private Map<PubSubTopicPartition, List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> prepareChangeCaptureRecordsToBePolled(
      String key,
      PubSubTopic changeCaptureTopic,
      int partition) {
    List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>> pubSubMessageList = new ArrayList<>();
    pubSubMessageList.add(constructChangeCaptureConsumerRecord(changeCaptureTopic, partition, key));
    Map<PubSubTopicPartition, List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> pubSubMessagesMap =
        new HashMap<>();
    PubSubTopicPartition topicPartition = new PubSubTopicPartitionImpl(changeCaptureTopic, partition);
    pubSubMessagesMap.put(topicPartition, pubSubMessageList);
    return pubSubMessagesMap;
  }

  private PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> constructChangeCaptureConsumerRecord(
      PubSubTopic changeCaptureVersionTopic,
      int partition,
      String key) {
    ChangeEvent<String> value = TEST_RECORDS.get(key);
    if (value == null) {
      throw new IllegalArgumentException("No test value exists for key " + key);
    }

    ValueBytes oldValueBytes = new ValueBytes();
    oldValueBytes.schemaId = TEST_SCHEMA_ID;
    oldValueBytes.value = ByteBuffer.wrap(valueSerializer.serialize(value.getPreviousValue()));
    ValueBytes newValueBytes = new ValueBytes();
    newValueBytes.schemaId = TEST_SCHEMA_ID;
    newValueBytes.value = ByteBuffer.wrap(valueSerializer.serialize(value.getCurrentValue()));
    RecordChangeEvent recordChangeEvent = new RecordChangeEvent();
    recordChangeEvent.currentValue = newValueBytes;
    recordChangeEvent.previousValue = oldValueBytes;
    recordChangeEvent.key = ByteBuffer.wrap(key.getBytes());
    recordChangeEvent.replicationCheckpointVector = Arrays.asList(1L, 1L);
    final RecordSerializer<RecordChangeEvent> recordChangeSerializer = FastSerializerDeserializerFactory
        .getFastAvroGenericSerializer(AvroProtocolDefinition.RECORD_CHANGE_EVENT.getCurrentProtocolVersionSchema());
    recordChangeSerializer.serialize(recordChangeEvent);
    KafkaMessageEnvelope kafkaMessageEnvelope = new KafkaMessageEnvelope(
        MessageType.PUT.getValue(),
        new ProducerMetadata(),
        new Put(
            ByteBuffer.wrap(recordChangeSerializer.serialize(recordChangeEvent)),
            TEST_SCHEMA_ID,
            0,
            ByteBuffer.allocate(0)),
        null);
    KafkaKey kafkaKey = new KafkaKey(MessageType.PUT, keySerializer.serialize(key));
    PubSubTopicPartition pubSubTopicPartition = new PubSubTopicPartitionImpl(changeCaptureVersionTopic, partition);
    return new ImmutablePubSubMessage<>(kafkaKey, kafkaMessageEnvelope, pubSubTopicPartition, 0, 0, 0);
  }
}
