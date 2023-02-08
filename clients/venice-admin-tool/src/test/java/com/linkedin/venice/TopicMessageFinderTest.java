package com.linkedin.venice;

import static com.linkedin.venice.kafka.protocol.enums.MessageType.PUT;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.venice.kafka.consumer.ApacheKafkaConsumer;
import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.serialization.avro.KafkaValueSerializer;
import com.linkedin.venice.serialization.avro.OptimizedKafkaValueSerializer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.testng.annotations.Test;


public class TopicMessageFinderTest {
  @Test
  public void testTopicMessageFinder() {
    ApacheKafkaConsumer apacheKafkaConsumer = mock(ApacheKafkaConsumer.class);
    String topic = "1_rt";
    int assignedPartition = 0;
    long startOffset = 0;
    long endOffset = 1;
    long progressInterval = 1;
    byte[] serializedKey = new byte[] { 0, 1, 2 };
    KafkaKeySerializer keySerializer = new KafkaKeySerializer();
    KafkaKey kafkaKey = new KafkaKey(PUT, serializedKey);
    byte[] serializedKafkaKey = keySerializer.serialize(topic, kafkaKey);
    KafkaValueSerializer valueSerializer = new OptimizedKafkaValueSerializer();
    KafkaMessageEnvelope messageEnvelope = new KafkaMessageEnvelope();
    messageEnvelope.producerMetadata = new ProducerMetadata();
    messageEnvelope.producerMetadata.messageTimestamp = 0;
    messageEnvelope.producerMetadata.messageSequenceNumber = 0;
    messageEnvelope.producerMetadata.segmentNumber = 0;
    messageEnvelope.producerMetadata.producerGUID = new GUID();
    Put put = new Put();
    put.putValue = ByteBuffer.allocate(0);
    put.replicationMetadataPayload = ByteBuffer.allocate(0);
    messageEnvelope.payloadUnion = put;
    byte[] serializedMessageEnvelope = valueSerializer.serialize(topic, messageEnvelope);

    ConsumerRecord<byte[], byte[]> consumerRecord1 =
        new ConsumerRecord<>(topic, assignedPartition, 0, serializedKafkaKey, serializedMessageEnvelope);
    ConsumerRecord<byte[], byte[]> consumerRecord2 =
        new ConsumerRecord<>(topic, assignedPartition, endOffset, new byte[] { 0, 1, 2, 3 }, new byte[] { 0, 1, 2, 3 });
    List<ConsumerRecord<byte[], byte[]>> consumerRecordList = new ArrayList<>();
    consumerRecordList.add(consumerRecord1);
    consumerRecordList.add(consumerRecord2);
    Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> recordsMap = new HashMap<>();
    recordsMap.put(new TopicPartition(topic, assignedPartition), consumerRecordList);
    ConsumerRecords<byte[], byte[]> records = new ConsumerRecords<>(recordsMap);
    when(apacheKafkaConsumer.poll(anyLong())).thenReturn(records, ConsumerRecords.empty());

    TopicMessageFinder.consume(
        apacheKafkaConsumer,
        topic,
        assignedPartition,
        startOffset,
        endOffset,
        progressInterval,
        serializedKey);
  }
}
