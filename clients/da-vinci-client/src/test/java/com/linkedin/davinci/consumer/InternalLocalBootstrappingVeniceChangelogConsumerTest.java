package com.linkedin.davinci.consumer;

import static com.linkedin.venice.ConfigKeys.CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.ZOOKEEPER_ADDRESS;
import static com.linkedin.venice.offsets.OffsetRecord.LOWEST_OFFSET;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.davinci.repository.ThinClientMetaStoreBasedRepository;
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
import com.linkedin.venice.pubsub.ImmutablePubSubMessage;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.schema.rmd.RmdSchemaGenerator;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.KafkaValueSerializer;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.views.ChangeCaptureView;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class InternalLocalBootstrappingVeniceChangelogConsumerTest {
  private static final String TEST_CLUSTER_NAME = "test_cluster";
  private static final String TEST_ZOOKEEPER_ADDRESS = "test_zookeeper";
  private static final String TEST_BOOTSTRAP_FILE_SYSTEM_PATH = "/export/content/data/change-capture";
  private String storeName;
  private RecordSerializer<String> keySerializer;
  private RecordSerializer<String> valueSerializer;
  private Schema rmdSchema;
  private SchemaReader schemaReader;
  private final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();

  @BeforeMethod
  public void setUp() {
    storeName = Utils.getUniqueString();
    schemaReader = mock(SchemaReader.class);
    Schema keySchema = AvroCompatibilityHelper.parse("\"string\"");
    doReturn(keySchema).when(schemaReader).getKeySchema();
    Schema valueSchema = AvroCompatibilityHelper.parse("\"string\"");
    doReturn(valueSchema).when(schemaReader).getValueSchema(1);
    rmdSchema = RmdSchemaGenerator.generateMetadataSchema(valueSchema, 1);

    keySerializer = FastSerializerDeserializerFactory.getFastAvroGenericSerializer(keySchema);
    valueSerializer = FastSerializerDeserializerFactory.getFastAvroGenericSerializer(valueSchema);
  }

  @Test
  public void testStart() throws ExecutionException, InterruptedException {
    D2ControllerClient d2ControllerClient = mock(D2ControllerClient.class);
    StoreResponse storeResponse = mock(StoreResponse.class);
    StoreInfo storeInfo = mock(StoreInfo.class);
    doReturn(1).when(storeInfo).getCurrentVersion();
    doReturn(2).when(storeInfo).getPartitionCount();
    doReturn(storeInfo).when(storeResponse).getStore();
    doReturn(storeResponse).when(d2ControllerClient).getStore(storeName);

    PubSubTopic versionTopic = pubSubTopicRepository.getTopic(Version.composeKafkaTopic(storeName, 1));
    PubSubTopic changeCaptureTopic =
        pubSubTopicRepository.getTopic(versionTopic.getName() + ChangeCaptureView.CHANGE_CAPTURE_TOPIC_SUFFIX);
    PubSubTopicPartition topicPartition_0 = new PubSubTopicPartitionImpl(changeCaptureTopic, 0);
    PubSubTopicPartition topicPartition_1 = new PubSubTopicPartitionImpl(changeCaptureTopic, 1);
    Set<PubSubTopicPartition> assignments = ImmutableSet.of(topicPartition_0, topicPartition_1);
    PubSubConsumerAdapter mockPubSubConsumer = mock(PubSubConsumerAdapter.class);
    doReturn(assignments).when(mockPubSubConsumer).getAssignment();
    doReturn(LOWEST_OFFSET).when(mockPubSubConsumer).getLatestOffset(topicPartition_0);
    doReturn(LOWEST_OFFSET).when(mockPubSubConsumer).getLatestOffset(topicPartition_1);
    doReturn(LOWEST_OFFSET).when(mockPubSubConsumer).endOffset(topicPartition_0);
    doReturn(LOWEST_OFFSET).when(mockPubSubConsumer).endOffset(topicPartition_1);
    when(mockPubSubConsumer.poll(anyLong())).thenReturn(new HashMap<>());

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
    InternalLocalBootstrappingVeniceChangelogConsumer<String, Utf8> bootstrappingVeniceChangelogConsumer =
        new InternalLocalBootstrappingVeniceChangelogConsumer<>(
            changelogClientConfig,
            mockPubSubConsumer,
            TEST_BOOTSTRAP_FILE_SYSTEM_PATH);

    ThinClientMetaStoreBasedRepository mockRepository = mock(ThinClientMetaStoreBasedRepository.class);
    Store store = mock(Store.class);
    Version mockVersion = new VersionImpl(storeName, 1, "foo");
    Mockito.when(store.getCurrentVersion()).thenReturn(1);
    Mockito.when(store.getCompressionStrategy()).thenReturn(CompressionStrategy.NO_OP);
    Mockito.when(mockRepository.getStore(anyString())).thenReturn(store);
    Mockito.when(store.getVersion(Mockito.anyInt())).thenReturn(Optional.of(mockVersion));
    bootstrappingVeniceChangelogConsumer.setStoreRepository(mockRepository);

    Map<PubSubTopicPartition, List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> polledResults_1 =
        prepareChangeCaptureRecordsToBePolled(0L, 1L, mockPubSubConsumer, changeCaptureTopic, 0);
    Map<PubSubTopicPartition, List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> polledResults_2 =
        prepareChangeCaptureRecordsToBePolled(0L, 1L, mockPubSubConsumer, changeCaptureTopic, 1);
    when(mockPubSubConsumer.poll(anyLong())).thenReturn(polledResults_1).thenReturn(polledResults_2);

    bootstrappingVeniceChangelogConsumer.start().get();

    verify(mockPubSubConsumer).subscribe(topicPartition_0, LOWEST_OFFSET);
    verify(mockPubSubConsumer).subscribe(topicPartition_1, LOWEST_OFFSET);
  }

  private Map<PubSubTopicPartition, List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> prepareChangeCaptureRecordsToBePolled(
      long startIdx,
      long endIdx,
      PubSubConsumerAdapter pubSubConsumer,
      PubSubTopic changeCaptureTopic,
      int partition) {
    List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>> pubSubMessageList = new ArrayList<>();

    Map<PubSubTopicPartition, List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> pubSubMessagesMap =
        new HashMap<>();
    for (long i = startIdx; i < endIdx; i++) {
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> pubSubMessage = constructChangeCaptureConsumerRecord(
          changeCaptureTopic,
          partition,
          "oldValue" + i,
          "newValue" + i,
          "key" + i,
          Arrays.asList(i, i));
      pubSubMessageList.add(pubSubMessage);
    }

    PubSubTopicPartition topicPartition = new PubSubTopicPartitionImpl(changeCaptureTopic, partition);
    pubSubMessagesMap.put(topicPartition, pubSubMessageList);
    return pubSubMessagesMap;
  }

  private PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> constructChangeCaptureConsumerRecord(
      PubSubTopic changeCaptureVersionTopic,
      int partition,
      String oldValue,
      String newValue,
      String key,
      List<Long> replicationCheckpointVector) {
    ValueBytes oldValueBytes = new ValueBytes();
    oldValueBytes.schemaId = 1;
    oldValueBytes.value = ByteBuffer.wrap(valueSerializer.serialize(oldValue));
    ValueBytes newValueBytes = new ValueBytes();
    newValueBytes.schemaId = 1;
    newValueBytes.value = ByteBuffer.wrap(valueSerializer.serialize(newValue));
    RecordChangeEvent recordChangeEvent = new RecordChangeEvent();
    recordChangeEvent.currentValue = newValueBytes;
    recordChangeEvent.previousValue = oldValueBytes;
    recordChangeEvent.key = ByteBuffer.wrap(key.getBytes());
    recordChangeEvent.replicationCheckpointVector = replicationCheckpointVector;
    final RecordSerializer<RecordChangeEvent> recordChangeSerializer = FastSerializerDeserializerFactory
        .getFastAvroGenericSerializer(AvroProtocolDefinition.RECORD_CHANGE_EVENT.getCurrentProtocolVersionSchema());
    recordChangeSerializer.serialize(recordChangeEvent);
    KafkaMessageEnvelope kafkaMessageEnvelope = new KafkaMessageEnvelope(
        MessageType.PUT.getValue(),
        new ProducerMetadata(),
        new Put(ByteBuffer.wrap(recordChangeSerializer.serialize(recordChangeEvent)), 0, 0, ByteBuffer.allocate(0)),
        null);
    KafkaKey kafkaKey = new KafkaKey(MessageType.PUT, keySerializer.serialize(key));
    PubSubTopicPartition pubSubTopicPartition = new PubSubTopicPartitionImpl(changeCaptureVersionTopic, partition);
    return new ImmutablePubSubMessage<>(kafkaKey, kafkaMessageEnvelope, pubSubTopicPartition, 0, 0, 0);
  }
}
