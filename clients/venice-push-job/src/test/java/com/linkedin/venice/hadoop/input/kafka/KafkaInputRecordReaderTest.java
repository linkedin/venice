package com.linkedin.venice.hadoop.input.kafka;

import static com.linkedin.venice.kafka.protocol.enums.MessageType.PUT;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_BROKER_URL;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_TOPIC;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_SOURCE_KEY_SCHEMA_STRING_PROP;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.venice.hadoop.input.kafka.avro.KafkaInputMapperKey;
import com.linkedin.venice.hadoop.input.kafka.avro.KafkaInputMapperValue;
import com.linkedin.venice.hadoop.input.kafka.avro.MapperValueType;
import com.linkedin.venice.hadoop.mapreduce.datawriter.task.ReporterBackedMapReduceDataWriterTaskTracker;
import com.linkedin.venice.hadoop.task.datawriter.DataWriterTaskTracker;
import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.ImmutablePubSubMessage;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.storage.protocol.ChunkedKeySuffix;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.vpj.pubsub.input.PubSubPartitionSplit;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.testng.Assert;
import org.testng.annotations.Test;


public class KafkaInputRecordReaderTest {
  private static final String KAFKA_MESSAGE_KEY_PREFIX = "key_";
  private static final String KAFKA_MESSAGE_VALUE_PREFIX = "value_";

  private static final PubSubTopicRepository TOPIC_REPOSITORY = new PubSubTopicRepository();

  @Test
  public void testNext() throws IOException {
    JobConf conf = new JobConf();
    conf.set(KAFKA_INPUT_BROKER_URL, "kafkaAddress");
    conf.set(KAFKA_SOURCE_KEY_SCHEMA_STRING_PROP, ChunkedKeySuffix.SCHEMA$.toString());
    String topic = "1_v1";
    conf.set(KAFKA_INPUT_TOPIC, topic);
    PubSubConsumerAdapter consumer = mock(PubSubConsumerAdapter.class);

    int assignedPartition = 0;
    int numRecord = 100;
    List<DefaultPubSubMessage> consumerRecordList = new ArrayList<>();
    PubSubTopicPartition pubSubTopicPartition =
        new PubSubTopicPartitionImpl(TOPIC_REPOSITORY.getTopic(topic), assignedPartition);
    for (int i = 0; i < numRecord; ++i) {
      if (i == 50) {
        // Simulate a gap in the data stream by skipping some records.
        continue;
      }

      byte[] keyBytes = (KAFKA_MESSAGE_KEY_PREFIX + i).getBytes();
      byte[] valueBytes = (KAFKA_MESSAGE_VALUE_PREFIX + i).getBytes();

      KafkaKey kafkaKey = new KafkaKey(PUT, keyBytes);
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
      consumerRecordList.add(
          new ImmutablePubSubMessage(
              kafkaKey,
              messageEnvelope,
              pubSubTopicPartition,
              ApacheKafkaOffsetPosition.of(i),
              -1,
              -1));
    }

    Map<PubSubTopicPartition, List<DefaultPubSubMessage>> recordsMap = new HashMap<>();
    recordsMap.put(pubSubTopicPartition, consumerRecordList);
    when(consumer.poll(anyLong())).thenReturn(recordsMap, new HashMap<>());
    PubSubTopicPartition topicPartition = new PubSubTopicPartitionImpl(TOPIC_REPOSITORY.getTopic(topic), 0);
    PubSubPosition startPosition = ApacheKafkaOffsetPosition.of(0L);
    PubSubPosition endPosition = ApacheKafkaOffsetPosition.of(100L);
    KafkaInputSplit split = new KafkaInputSplit(
        new PubSubPartitionSplit(TOPIC_REPOSITORY, topicPartition, startPosition, endPosition, numRecord, 0, 0L));
    DataWriterTaskTracker taskTracker = new ReporterBackedMapReduceDataWriterTaskTracker(Reporter.NULL);

    doAnswer(invocation -> {
      PubSubPosition pos1 = invocation.getArgument(1);
      PubSubPosition pos2 = invocation.getArgument(2);
      long offset1 = ((ApacheKafkaOffsetPosition) pos1).getInternalOffset();
      long offset2 = ((ApacheKafkaOffsetPosition) pos2).getInternalOffset();
      return offset1 - offset2;
    }).when(consumer).positionDifference(any(), any(), any());

    try (KafkaInputRecordReader reader = new KafkaInputRecordReader(split, conf, taskTracker, consumer)) {
      for (int i = 0; i < (numRecord + 10); ++i) {
        if (i >= numRecord) {
          // If cursor is beyond the number of records, it should not have any pending data.
          Assert.assertFalse(reader.hasPendingData());
          continue;
        } else if (i == 50) {
          Assert
              .assertTrue(reader.hasPendingData(), "Reader should have pending data after " + i + "th call to next()");
          // due to the gap in the data stream, it should skip this record.
          continue;
        } else {
          Assert.assertTrue(reader.hasPendingData(), "Reader should have pending data at index " + i);
        }
        KafkaInputMapperKey key = new KafkaInputMapperKey();
        KafkaInputMapperValue value = new KafkaInputMapperValue();
        reader.next(key, value);
        Assert.assertEquals(key.key.array(), (KAFKA_MESSAGE_KEY_PREFIX + i).getBytes(), "Key mismatch at index " + i);
        Assert.assertEquals(value.offset, i);
        Assert.assertEquals(value.schemaId, -1);
        Assert.assertEquals(value.valueType, MapperValueType.PUT);
        Assert.assertEquals(ByteUtils.extractByteArray(value.value), (KAFKA_MESSAGE_VALUE_PREFIX + i).getBytes());
      }
    }
  }
}
