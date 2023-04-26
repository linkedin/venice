package com.linkedin.davinci.consumer;

import static org.mockito.Mockito.*;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.davinci.repository.ThinClientMetaStoreBasedRepository;
import com.linkedin.venice.client.change.capture.protocol.RecordChangeEvent;
import com.linkedin.venice.client.change.capture.protocol.ValueBytes;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.D2ControllerClient;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.EndOfPush;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.StartOfPush;
import com.linkedin.venice.kafka.protocol.VersionSwap;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.schema.rmd.RmdConstants;
import com.linkedin.venice.schema.rmd.RmdSchemaGenerator;
import com.linkedin.venice.schema.rmd.RmdUtils;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.views.ChangeCaptureView;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class VeniceChangelogConsumerImplTest {
  private String storeName;
  private RecordSerializer<String> keySerializer;
  private RecordSerializer<String> valueSerializer;
  private Schema rmdSchema;
  private SchemaReader schemaReader;

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
  public void testConsumeBeforeAndAfterImage() throws ExecutionException, InterruptedException {
    D2ControllerClient d2ControllerClient = mock(D2ControllerClient.class);
    StoreResponse storeResponse = mock(StoreResponse.class);
    StoreInfo store = mock(StoreInfo.class);
    doReturn(1).when(store).getCurrentVersion();
    doReturn(2).when(store).getPartitionCount();
    doReturn(store).when(storeResponse).getStore();
    doReturn(storeResponse).when(d2ControllerClient).getStore(storeName);

    Consumer<KafkaKey, KafkaMessageEnvelope> mockKafkaConsumer = mock(Consumer.class);
    doReturn(new HashSet<>()).when(mockKafkaConsumer).assignment();
    String newVersionTopic = Version.composeKafkaTopic(storeName, 2);
    String oldVersionTopic = Version.composeKafkaTopic(storeName, 1);
    String newChangeCaptureTopic = newVersionTopic + ChangeCaptureView.CHANGE_CAPTURE_TOPIC_SUFFIX;
    String oldChangeCaptureTopic = oldVersionTopic + ChangeCaptureView.CHANGE_CAPTURE_TOPIC_SUFFIX;

    int partition = 0;
    prepareChangeCaptureRecordsToBePolled(
        0L,
        5L,
        mockKafkaConsumer,
        oldChangeCaptureTopic,
        partition,
        oldVersionTopic,
        newVersionTopic);
    ChangelogClientConfig changelogClientConfig =
        new ChangelogClientConfig<>().setD2ControllerClient(d2ControllerClient)
            .setSchemaReader(schemaReader)
            .setStoreName(storeName)
            .setViewName("changeCaptureView");
    VeniceChangelogConsumerImpl<String, Utf8> veniceChangelogConsumer =
        new VeniceChangelogConsumerImpl<>(changelogClientConfig, mockKafkaConsumer);
    ThinClientMetaStoreBasedRepository mockRepository = mock(ThinClientMetaStoreBasedRepository.class);
    veniceChangelogConsumer.setReadOnlySchemaRepository(mockRepository);
    veniceChangelogConsumer.subscribe(new HashSet<>(Arrays.asList(0))).get();
    verify(mockKafkaConsumer).assign(Arrays.asList(new TopicPartition(oldVersionTopic, 0)));

    List<PubSubMessage<String, ChangeEvent<Utf8>, Long>> pubSubMessages =
        (List<PubSubMessage<String, ChangeEvent<Utf8>, Long>>) veniceChangelogConsumer.poll(100);
    for (int i = 0; i < 5; i++) {
      PubSubMessage<String, ChangeEvent<Utf8>, Long> pubSubMessage = pubSubMessages.get(i);
      ChangeEvent<Utf8> changeEvent = pubSubMessage.getValue();
      Assert.assertEquals(changeEvent.getCurrentValue().toString(), "newValue" + i);
      Assert.assertEquals(changeEvent.getPreviousValue().toString(), "oldValue" + i);
    }
    // Verify version swap happened.
    verify(mockKafkaConsumer).assign(Arrays.asList(new TopicPartition(newChangeCaptureTopic, 0)));
    pubSubMessages = (List<PubSubMessage<String, ChangeEvent<Utf8>, Long>>) veniceChangelogConsumer.poll(100);
    Assert.assertTrue(pubSubMessages.isEmpty());
  }

  @Test
  public void testConsumeAfterImage() throws ExecutionException, InterruptedException {
    D2ControllerClient d2ControllerClient = mock(D2ControllerClient.class);
    StoreResponse storeResponse = mock(StoreResponse.class);
    StoreInfo store = mock(StoreInfo.class);
    doReturn(1).when(store).getCurrentVersion();
    doReturn(2).when(store).getPartitionCount();
    doReturn(store).when(storeResponse).getStore();
    doReturn(storeResponse).when(d2ControllerClient).getStore(storeName);
    MultiSchemaResponse multiRMDSchemaResponse = mock(MultiSchemaResponse.class);
    MultiSchemaResponse.Schema rmdSchemaFromMultiSchemaResponse = mock(MultiSchemaResponse.Schema.class);
    doReturn(rmdSchema.toString()).when(rmdSchemaFromMultiSchemaResponse).getSchemaStr();
    doReturn(new MultiSchemaResponse.Schema[] { rmdSchemaFromMultiSchemaResponse }).when(multiRMDSchemaResponse)
        .getSchemas();
    doReturn(multiRMDSchemaResponse).when(d2ControllerClient).getAllReplicationMetadataSchemas(storeName);

    Consumer<KafkaKey, KafkaMessageEnvelope> kafkaConsumer = mock(Consumer.class);
    String oldVersionTopic = Version.composeKafkaTopic(storeName, 1);
    String oldChangeCaptureTopic = oldVersionTopic + ChangeCaptureView.CHANGE_CAPTURE_TOPIC_SUFFIX;

    prepareVersionTopicRecordsToBePolled(0L, 5L, kafkaConsumer, oldVersionTopic, 0, true);
    ChangelogClientConfig changelogClientConfig =
        new ChangelogClientConfig<>().setD2ControllerClient(d2ControllerClient)
            .setSchemaReader(schemaReader)
            .setStoreName(storeName)
            .setViewName("");
    VeniceChangelogConsumerImpl<String, Utf8> veniceChangelogConsumer =
        new VeniceAfterImageConsumerImpl<>(changelogClientConfig, kafkaConsumer);
    ThinClientMetaStoreBasedRepository mockRepository = mock(ThinClientMetaStoreBasedRepository.class);
    veniceChangelogConsumer.setReadOnlySchemaRepository(mockRepository);
    veniceChangelogConsumer.subscribe(new HashSet<>(Arrays.asList(0))).get();
    verify(kafkaConsumer).assign(Arrays.asList(new TopicPartition(oldVersionTopic, 0)));

    List<PubSubMessage<String, ChangeEvent<Utf8>, Long>> pubSubMessages =
        (List<PubSubMessage<String, ChangeEvent<Utf8>, Long>>) veniceChangelogConsumer.poll(100);
    for (int i = 0; i < 5; i++) {
      PubSubMessage<String, ChangeEvent<Utf8>, Long> pubSubMessage = pubSubMessages.get(i);
      Utf8 messageStr = pubSubMessage.getValue().getCurrentValue();
      Assert.assertEquals(messageStr.toString(), "newValue" + i);
    }
    // Verify version swap from version topic to its corresponding change capture topic happened.
    verify(kafkaConsumer).assign(Arrays.asList(new TopicPartition(oldChangeCaptureTopic, 0)));
    prepareChangeCaptureRecordsToBePolled(0L, 10L, kafkaConsumer, oldChangeCaptureTopic, 0, oldVersionTopic, "");
    pubSubMessages = (List<PubSubMessage<String, ChangeEvent<Utf8>, Long>>) veniceChangelogConsumer.poll(100);
    Assert.assertFalse(pubSubMessages.isEmpty());
    Assert.assertEquals(pubSubMessages.size(), 10);
    for (int i = 0; i < 10; i++) {
      PubSubMessage<String, ChangeEvent<Utf8>, Long> pubSubMessage = pubSubMessages.get(i);
      Utf8 pubSubMessageValue = pubSubMessage.getValue().getCurrentValue();
      Assert.assertEquals(pubSubMessageValue.toString(), "newValue" + i);
    }

    veniceChangelogConsumer.close();
    verify(kafkaConsumer, times(2)).assign(any());
    verify(kafkaConsumer).close();
  }

  private void prepareChangeCaptureRecordsToBePolled(
      long startIdx,
      long endIdx,
      Consumer kafkaConsumer,
      String changeCaptureTopic,
      int partition,
      String oldVersionTopic,
      String newVersionTopic) {
    List<ConsumerRecord<KafkaKey, KafkaMessageEnvelope>> consumerRecordList = new ArrayList<>();

    // Add a start of push message
    consumerRecordList.add(constructStartOfPushMessage(oldVersionTopic, partition));

    Map<TopicPartition, List<ConsumerRecord<KafkaKey, KafkaMessageEnvelope>>> consumerRecordsMap = new HashMap<>();
    for (long i = startIdx; i < endIdx; i++) {
      ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord = constructChangeCaptureConsumerRecord(
          changeCaptureTopic,
          partition,
          "oldValue" + i,
          "newValue" + i,
          "key" + i,
          Arrays.asList(i, i));
      consumerRecordList.add(consumerRecord);
    }
    if (!newVersionTopic.isEmpty()) {
      consumerRecordList.add(
          constructVersionSwapMessage(
              oldVersionTopic,
              oldVersionTopic,
              newVersionTopic,
              partition,
              Arrays.asList(Long.valueOf(endIdx), Long.valueOf(endIdx))));
    }
    TopicPartition topicPartition = new TopicPartition(changeCaptureTopic, partition);
    consumerRecordsMap.put(topicPartition, consumerRecordList);
    ConsumerRecords<KafkaKey, KafkaMessageEnvelope> polledConsumerRecords = new ConsumerRecords<>(consumerRecordsMap);
    doReturn(polledConsumerRecords).when(kafkaConsumer).poll(100);
  }

  private void prepareVersionTopicRecordsToBePolled(
      long startIdx,
      long endIdx,
      Consumer kafkaConsumer,
      String versionTopic,
      int partition,
      boolean prepareEndOfPush) {
    List<ConsumerRecord<KafkaKey, KafkaMessageEnvelope>> consumerRecordList = new ArrayList<>();
    Map<TopicPartition, List<ConsumerRecord<KafkaKey, KafkaMessageEnvelope>>> consumerRecordsMap = new HashMap<>();
    for (long i = startIdx; i < endIdx; i++) {
      ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord =
          constructConsumerRecord(versionTopic, partition, "newValue" + i, "key" + i, Arrays.asList(i, i));
      consumerRecordList.add(consumerRecord);
    }
    if (prepareEndOfPush) {
      consumerRecordList.add(constructEndOfPushMessage(versionTopic, partition));
    }
    TopicPartition topicPartition = new TopicPartition(versionTopic, partition);
    consumerRecordsMap.put(topicPartition, consumerRecordList);
    ConsumerRecords<KafkaKey, KafkaMessageEnvelope> polledConsumerRecords = new ConsumerRecords<>(consumerRecordsMap);
    doReturn(polledConsumerRecords).when(kafkaConsumer).poll(100);
  }

  private ConsumerRecord<KafkaKey, KafkaMessageEnvelope> constructVersionSwapMessage(
      String versionTopic,
      String oldTopic,
      String newTopic,
      int partition,
      List<Long> localHighWatermarks) {
    KafkaKey kafkaKey = new KafkaKey(MessageType.CONTROL_MESSAGE, null);
    VersionSwap versionSwapMessage = new VersionSwap();
    versionSwapMessage.oldServingVersionTopic = oldTopic;
    versionSwapMessage.newServingVersionTopic = newTopic;
    versionSwapMessage.localHighWatermarks = localHighWatermarks;

    KafkaMessageEnvelope kafkaMessageEnvelope = new KafkaMessageEnvelope();
    ControlMessage controlMessage = new ControlMessage();
    controlMessage.controlMessageUnion = versionSwapMessage;
    controlMessage.controlMessageType = ControlMessageType.VERSION_SWAP.getValue();
    kafkaMessageEnvelope.payloadUnion = controlMessage;
    return new ConsumerRecord<>(versionTopic, partition, 0, kafkaKey, kafkaMessageEnvelope);
  }

  private ConsumerRecord<KafkaKey, KafkaMessageEnvelope> constructChangeCaptureConsumerRecord(
      String changeCaptureVersionTopic,
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
    return new ConsumerRecord<>(changeCaptureVersionTopic, partition, 0, kafkaKey, kafkaMessageEnvelope);
  }

  private ConsumerRecord<KafkaKey, KafkaMessageEnvelope> constructConsumerRecord(
      String changeCaptureVersionTopic,
      int partition,
      String newValue,
      String key,
      List<Long> replicationCheckpointVector) {
    final GenericRecord rmdRecord = new GenericData.Record(rmdSchema);
    rmdRecord.put(RmdConstants.TIMESTAMP_FIELD_NAME, 0L);
    rmdRecord.put(RmdConstants.REPLICATION_CHECKPOINT_VECTOR_FIELD, replicationCheckpointVector);
    ByteBuffer bytes = RmdUtils.serializeRmdRecord(rmdSchema, rmdRecord);
    KafkaMessageEnvelope kafkaMessageEnvelope = new KafkaMessageEnvelope(
        MessageType.PUT.getValue(),
        new ProducerMetadata(),
        new Put(ByteBuffer.wrap(valueSerializer.serialize(newValue)), 1, 1, bytes),
        null);
    KafkaKey kafkaKey = new KafkaKey(MessageType.PUT, keySerializer.serialize(key));
    return new ConsumerRecord<>(changeCaptureVersionTopic, partition, 0, kafkaKey, kafkaMessageEnvelope);
  }

  private ConsumerRecord<KafkaKey, KafkaMessageEnvelope> constructEndOfPushMessage(String versionTopic, int partition) {
    KafkaKey kafkaKey = new KafkaKey(MessageType.CONTROL_MESSAGE, null);
    EndOfPush endOfPush = new EndOfPush();
    KafkaMessageEnvelope kafkaMessageEnvelope = new KafkaMessageEnvelope();
    ControlMessage controlMessage = new ControlMessage();
    controlMessage.controlMessageUnion = endOfPush;
    controlMessage.controlMessageType = ControlMessageType.END_OF_PUSH.getValue();
    kafkaMessageEnvelope.payloadUnion = controlMessage;
    return new ConsumerRecord<>(versionTopic, partition, 0, kafkaKey, kafkaMessageEnvelope);
  }

  private ConsumerRecord<KafkaKey, KafkaMessageEnvelope> constructStartOfPushMessage(
      String versionTopic,
      int partition) {
    KafkaKey kafkaKey = new KafkaKey(MessageType.CONTROL_MESSAGE, null);
    StartOfPush startOfPush = new StartOfPush();
    startOfPush.compressionStrategy = CompressionStrategy.NO_OP.getValue();
    KafkaMessageEnvelope kafkaMessageEnvelope = new KafkaMessageEnvelope();
    ControlMessage controlMessage = new ControlMessage();
    controlMessage.controlMessageUnion = startOfPush;
    controlMessage.controlMessageType = ControlMessageType.START_OF_PUSH.getValue();
    kafkaMessageEnvelope.payloadUnion = controlMessage;
    return new ConsumerRecord<>(versionTopic, partition, 0, kafkaKey, kafkaMessageEnvelope);
  }
}
