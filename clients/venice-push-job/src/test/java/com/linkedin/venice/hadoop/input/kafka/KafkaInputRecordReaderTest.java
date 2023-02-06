package com.linkedin.venice.hadoop.input.kafka;

import static com.linkedin.venice.hadoop.VenicePushJob.KAFKA_INPUT_BROKER_URL;
import static com.linkedin.venice.hadoop.VenicePushJob.KAFKA_INPUT_TOPIC;
import static com.linkedin.venice.kafka.protocol.enums.MessageType.PUT;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.hadoop.input.kafka.avro.KafkaInputMapperKey;
import com.linkedin.venice.hadoop.input.kafka.avro.KafkaInputMapperValue;
import com.linkedin.venice.hadoop.input.kafka.avro.MapperValueType;
import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.consumer.PubSubConsumer;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.serialization.avro.KafkaValueSerializer;
import com.linkedin.venice.serialization.avro.OptimizedKafkaValueSerializer;
import com.linkedin.venice.storage.protocol.ChunkedKeySuffix;
import com.linkedin.venice.utils.ByteUtils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.mapred.JobConf;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.testng.Assert;
import org.testng.annotations.Test;


public class KafkaInputRecordReaderTest {
  private static final String KAFKA_MESSAGE_KEY_PREFIX = "key_";
  private static final String KAFKA_MESSAGE_VALUE_PREFIX = "value_";

  private static final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();

  @Test
  public void testNext() throws IOException {
    JobConf conf = new JobConf();
    conf.set(KAFKA_INPUT_BROKER_URL, "kafkaAddress");
    conf.set(VenicePushJob.KAFKA_SOURCE_KEY_SCHEMA_STRING_PROP, ChunkedKeySuffix.SCHEMA$.toString());
    String topic = "1_v1";
    conf.set(KAFKA_INPUT_TOPIC, topic);
    PubSubConsumer consumer = mock(PubSubConsumer.class);

    int assignedPartition = 0;
    int numRecord = 100;
    List<ConsumerRecord<byte[], byte[]>> consumerRecordList = new ArrayList<>();

    for (int i = 0; i < numRecord; ++i) {
      byte[] keyBytes = (KAFKA_MESSAGE_KEY_PREFIX + i).getBytes();
      byte[] valueBytes = (KAFKA_MESSAGE_VALUE_PREFIX + i).getBytes();

      KafkaKeySerializer keySerializer = new KafkaKeySerializer();
      KafkaKey kafkaKey = new KafkaKey(PUT, keyBytes);
      byte[] serializedKafkaKey = keySerializer.serialize(topic, kafkaKey);
      KafkaValueSerializer valueSerializer = new OptimizedKafkaValueSerializer();
      KafkaMessageEnvelope messageEnvelope = new KafkaMessageEnvelope();
      messageEnvelope.producerMetadata = new ProducerMetadata();
      messageEnvelope.producerMetadata.messageTimestamp = 0;
      messageEnvelope.producerMetadata.messageSequenceNumber = 0;
      messageEnvelope.producerMetadata.segmentNumber = 0;
      messageEnvelope.producerMetadata.producerGUID = new GUID();
      Put put = new Put();
      put.schemaId = -1;
      put.putValue = ByteBuffer.wrap(valueBytes);
      put.replicationMetadataPayload = ByteBuffer.allocate(0);
      messageEnvelope.payloadUnion = put;
      byte[] serializedMessageEnvelope = valueSerializer.serialize(topic, messageEnvelope);

      ConsumerRecord<byte[], byte[]> consumerRecord =
          new ConsumerRecord<>(topic, assignedPartition, i, serializedKafkaKey, serializedMessageEnvelope);

      consumerRecordList.add(consumerRecord);
    }

    Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> recordsMap = new HashMap<>();
    recordsMap.put(new TopicPartition(topic, assignedPartition), consumerRecordList);
    ConsumerRecords<byte[], byte[]> records = new ConsumerRecords<>(recordsMap);
    when(consumer.poll(anyLong())).thenReturn(records, ConsumerRecords.empty());

    KafkaInputSplit split = new KafkaInputSplit(topic, 0, 0, 102);
    try (KafkaInputRecordReader reader =
        new KafkaInputRecordReader(split, conf, null, consumer, pubSubTopicRepository)) {
      for (int i = 0; i < numRecord; ++i) {
        KafkaInputMapperKey key = new KafkaInputMapperKey();
        KafkaInputMapperValue value = new KafkaInputMapperValue();
        reader.next(key, value);
        Assert.assertEquals(key.key.array(), (KAFKA_MESSAGE_KEY_PREFIX + i).getBytes());
        Assert.assertEquals(value.offset, i);
        Assert.assertEquals(value.schemaId, -1);
        Assert.assertEquals(value.valueType, MapperValueType.PUT);
        Assert.assertEquals(ByteUtils.extractByteArray(value.value), (KAFKA_MESSAGE_VALUE_PREFIX + i).getBytes());
      }
    }
  }
}
