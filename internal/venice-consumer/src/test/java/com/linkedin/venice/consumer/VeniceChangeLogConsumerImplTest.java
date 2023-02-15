package com.linkedin.venice.consumer;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.client.change.capture.protocol.RecordChangeEvent;
import com.linkedin.venice.client.change.capture.protocol.ValueBytes;
import com.linkedin.venice.client.consumer.ChangelogClientConfig;
import com.linkedin.venice.helix.HelixReadOnlyStoreRepository;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.VersionSwap;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.KafkaValueSerializer;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.views.ChangeCaptureView;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class VeniceChangeLogConsumerImplTest {
  @BeforeMethod
  public void setUp() {
  }

  @Test
  public void testConsumeChangeEvents() {
    String storeName = "test_store";
    HelixReadOnlyStoreRepository storeRepo = mock(HelixReadOnlyStoreRepository.class);
    Store store = mock(Store.class);
    doReturn(1).when(store).getCurrentVersion();
    doReturn(store).when(storeRepo).getStore(storeName);

    SchemaReader schemaReader = mock(SchemaReader.class);
    Schema keySchema = AvroCompatibilityHelper.parse("\"string\"");
    doReturn(keySchema).when(schemaReader).getKeySchema();
    Schema valueSchema = AvroCompatibilityHelper.parse("\"string\"");
    doReturn(valueSchema).when(schemaReader).getValueSchema(1);
    Properties consumerProperties = new Properties();
    consumerProperties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localKafkaUrl");
    consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaKeySerializer.class);
    consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaValueSerializer.class);
    consumerProperties.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 1024 * 1024);
    ChangelogClientConfig changelogClientConfig = new ChangelogClientConfig<>().setStoreRepo(storeRepo)
        .setSchemaReader(schemaReader)
        .setStoreName(storeName)
        .setViewClassName(ChangeCaptureView.class.getCanonicalName())
        .setConsumerProperties(consumerProperties);
    Consumer<KafkaKey, KafkaMessageEnvelope> kafkaConsumer = mock(Consumer.class);

    String versionTopic = Version.composeKafkaTopic(storeName, 1);
    int partition = 0;
    ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord =
        constructConsumerRecord(versionTopic, partition, "newValue", "oldValue", "key1");
    // doReturn().when(kafkaConsumer).poll(100);
  }

  private ConsumerRecord<KafkaKey, KafkaMessageEnvelope> constructControlConsumerRecord(
      String versionTopic,
      int partition,
      String newValue,
      String oldValue,
      String key) {

    KafkaKey kafkaKey = new KafkaKey(MessageType.CONTROL_MESSAGE, null);
    VersionSwap versionSwapMessage = new VersionSwap();
    versionSwapMessage.oldServingVersionTopic = Version.composeKafkaTopic("storeName", 1);
    versionSwapMessage.newServingVersionTopic = Version.composeKafkaTopic("storeName", 2);
    KafkaMessageEnvelope kafkaMessageEnvelope = new KafkaMessageEnvelope();

    return new ConsumerRecord<>(versionTopic, partition, 0, kafkaKey, kafkaMessageEnvelope);
  }

  private ConsumerRecord<KafkaKey, KafkaMessageEnvelope> constructConsumerRecord(
      String versionTopic,
      int partition,
      String newValue,
      String oldValue,
      String key) {
    ValueBytes oldValueBytes = new ValueBytes();
    oldValueBytes.schemaId = 1;
    oldValueBytes.value = ByteBuffer.wrap(oldValue.getBytes());
    ValueBytes newValueBytes = new ValueBytes();
    newValueBytes.schemaId = 1;
    newValueBytes.value = ByteBuffer.wrap(newValue.getBytes());
    RecordChangeEvent recordChangeEvent = new RecordChangeEvent();
    recordChangeEvent.currentValue = oldValueBytes;
    recordChangeEvent.previousValue = newValueBytes;
    recordChangeEvent.key = ByteBuffer.wrap(key.getBytes());
    recordChangeEvent.replicationCheckpointVector = new ArrayList<>();
    final RecordSerializer<RecordChangeEvent> recordChangeSerializer = FastSerializerDeserializerFactory
        .getFastAvroGenericSerializer(AvroProtocolDefinition.RECORD_CHANGE_EVENT.getCurrentProtocolVersionSchema());
    recordChangeSerializer.serialize(recordChangeEvent);

    KafkaMessageEnvelope kafkaMessageEnvelope = new KafkaMessageEnvelope(
        MessageType.PUT.getValue(),
        new ProducerMetadata(),
        new Put(ByteBuffer.wrap(recordChangeSerializer.serialize(recordChangeEvent)), 0, 0, ByteBuffer.allocate(0)),
        null);
    KafkaKey kafkaKey = new KafkaKey(MessageType.PUT, key.getBytes());
    return new ConsumerRecord<>(versionTopic, partition, 0, kafkaKey, kafkaMessageEnvelope);
  }
}
