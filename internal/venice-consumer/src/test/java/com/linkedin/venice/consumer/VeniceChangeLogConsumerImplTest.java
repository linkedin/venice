package com.linkedin.venice.consumer;

import static org.mockito.Mockito.*;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.client.change.capture.protocol.RecordChangeEvent;
import com.linkedin.venice.client.change.capture.protocol.ValueBytes;
import com.linkedin.venice.client.consumer.ChangelogClientConfig;
import com.linkedin.venice.client.consumer.VeniceChangeLogConsumerImpl;
import com.linkedin.venice.controllerapi.D2ControllerClient;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.VersionSwap;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.views.ChangeCaptureView;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class VeniceChangeLogConsumerImplTest {
  private String storeName = "test_store";
  private RecordSerializer<String> keySerializer;
  private RecordSerializer<String> valueSerializer;

  private SchemaReader schemaReader;

  @BeforeMethod
  public void setUp() {

    schemaReader = mock(SchemaReader.class);
    Schema keySchema = AvroCompatibilityHelper.parse("\"string\"");
    doReturn(keySchema).when(schemaReader).getKeySchema();
    Schema valueSchema = AvroCompatibilityHelper.parse("\"string\"");
    doReturn(valueSchema).when(schemaReader).getValueSchema(1);

    keySerializer = FastSerializerDeserializerFactory.getFastAvroGenericSerializer(keySchema);
    valueSerializer = FastSerializerDeserializerFactory.getFastAvroGenericSerializer(valueSchema);
  }

  @Test
  public void testConsumeChangeEvents() {

    D2ControllerClient d2ControllerClient = mock(D2ControllerClient.class);
    StoreResponse storeResponse = mock(StoreResponse.class);
    StoreInfo store = mock(StoreInfo.class);
    doReturn(1).when(store).getCurrentVersion();
    doReturn(2).when(store).getPartitionCount();
    doReturn(store).when(storeResponse).getStore();
    doReturn(storeResponse).when(d2ControllerClient).getStore(storeName);

    Consumer<KafkaKey, KafkaMessageEnvelope> kafkaConsumer = mock(Consumer.class);
    String newVersionTopic = Version.composeKafkaTopic(storeName, 2);
    String oldVersionTopic = Version.composeKafkaTopic(storeName, 1);
    String newChangeCaptureTopic = newVersionTopic + ChangeCaptureView.CHANGE_CAPTURE_TOPIC_SUFFIX;
    String oldChangeCaptureTopic = oldVersionTopic + ChangeCaptureView.CHANGE_CAPTURE_TOPIC_SUFFIX;

    int partition = 0;
    TopicPartition topicPartition = new TopicPartition(oldVersionTopic, partition);
    List<ConsumerRecord<KafkaKey, KafkaMessageEnvelope>> consumerRecordList = new ArrayList<>();
    for (Long i = 0L; i < 5; i++) {
      ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord = constructChangeCaptureConsumerRecord(
          oldVersionTopic,
          partition,
          "oldValue" + i,
          "newValue" + i,
          "key" + i,
          Arrays.asList(i, i));
      consumerRecordList.add(consumerRecord);
    }
    consumerRecordList.add(
        constructVersionSwapMessage(
            oldVersionTopic,
            oldVersionTopic,
            newVersionTopic,
            partition,
            Arrays.asList(5L, 5L)));
    Map<TopicPartition, List<ConsumerRecord<KafkaKey, KafkaMessageEnvelope>>> consumerRecordsMap = new HashMap<>();
    consumerRecordsMap.put(topicPartition, consumerRecordList);
    ConsumerRecords<KafkaKey, KafkaMessageEnvelope> polledConsumerRecords = new ConsumerRecords<>(consumerRecordsMap);
    doReturn(polledConsumerRecords).when(kafkaConsumer).poll(100);

    ChangelogClientConfig changelogClientConfig =
        new ChangelogClientConfig<>().setD2ControllerClient(d2ControllerClient)
            .setSchemaReader(schemaReader)
            .setStoreName(storeName)
            .setViewClassName(ChangeCaptureView.class.getCanonicalName());
    VeniceChangeLogConsumerImpl<String, String> veniceChangeLogConsumer =
        new VeniceChangeLogConsumerImpl<>(changelogClientConfig, kafkaConsumer);
    veniceChangeLogConsumer.subscribe(new HashSet<>(Arrays.asList(0)));
    verify(kafkaConsumer).assign(Arrays.asList(new TopicPartition(oldChangeCaptureTopic, 0)));

    List<PubSubMessage> pubSubMessages = (List<PubSubMessage>) veniceChangeLogConsumer.poll(100);
    for (int i = 0; i < 5; i++) {
      PubSubMessage pubSubMessage = pubSubMessages.get(i);
      VeniceChangeLogConsumerImpl.ChangeEvent<Utf8> changeEvent =
          (VeniceChangeLogConsumerImpl.ChangeEvent<Utf8>) pubSubMessage.getValue();
      Assert.assertEquals(changeEvent.getCurrentValue().toString(), "newValue" + i);
      Assert.assertEquals(changeEvent.getPreviousValue().toString(), "oldValue" + i);
    }
    // Verify version swap happened.
    verify(kafkaConsumer).assign(Arrays.asList(new TopicPartition(newChangeCaptureTopic, 0)));
    pubSubMessages = (List<PubSubMessage>) veniceChangeLogConsumer.poll(100);
    Assert.assertTrue(pubSubMessages.isEmpty());
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
}
