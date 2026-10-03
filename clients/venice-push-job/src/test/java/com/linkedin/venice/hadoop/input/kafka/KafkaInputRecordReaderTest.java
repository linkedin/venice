package com.linkedin.venice.hadoop.input.kafka;

import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.PUBSUB_CONSUMER_ADAPTER_FACTORY_CLASS;
import static com.linkedin.venice.kafka.protocol.enums.MessageType.PUT;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_TOPIC;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_SOURCE_KEY_SCHEMA_STRING_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.PUB_SUB_ENCRYPTION_ENABLED;
import static com.linkedin.venice.vpj.VenicePushJobConstants.PUB_SUB_ENCRYPTION_KEY_URN;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VENICE_REPUSH_SOURCE_PUBSUB_BROKER;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.venice.exceptions.VeniceException;
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
import com.linkedin.venice.vpj.PubSubEncryptionUtilsTest;
import com.linkedin.venice.vpj.pubsub.input.PubSubPartitionSplit;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
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
    conf.set(VENICE_REPUSH_SOURCE_PUBSUB_BROKER, "kafkaAddress");
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
    verify(consumer, never()).close();
  }

  @Test(dataProvider = "encryptionConfigurations", dataProviderClass = PubSubEncryptionUtilsTest.class)
  public void testCreateConsumerUsesEncryptionFlag(Boolean enabled, String keyUrn, boolean invalid) throws IOException {
    KafkaInputUtilsTest.RecordingPubSubConsumerAdapterFactory.reset();

    JobConf conf = new JobConf();
    conf.set(VENICE_REPUSH_SOURCE_PUBSUB_BROKER, "kafkaAddress");
    conf.set(KAFKA_BOOTSTRAP_SERVERS, "kafkaAddress");
    conf.set(KAFKA_SOURCE_KEY_SCHEMA_STRING_PROP, ChunkedKeySuffix.SCHEMA$.toString());
    String topic = "1_v1";
    conf.set(KAFKA_INPUT_TOPIC, topic);
    conf.set(
        PUBSUB_CONSUMER_ADAPTER_FACTORY_CLASS,
        KafkaInputUtilsTest.RecordingPubSubConsumerAdapterFactory.class.getName());
    if (enabled != null) {
      conf.setBoolean(PUB_SUB_ENCRYPTION_ENABLED, enabled);
    }
    if (keyUrn != null) {
      conf.set(PUB_SUB_ENCRYPTION_KEY_URN, keyUrn);
    }

    PubSubTopicPartition topicPartition = new PubSubTopicPartitionImpl(TOPIC_REPOSITORY.getTopic(topic), 0);
    PubSubPosition startPosition = ApacheKafkaOffsetPosition.of(0L);
    PubSubPosition endPosition = ApacheKafkaOffsetPosition.of(1L);
    KafkaInputSplit split = new KafkaInputSplit(
        new PubSubPartitionSplit(TOPIC_REPOSITORY, topicPartition, startPosition, endPosition, 1, 0, 0L));
    DataWriterTaskTracker taskTracker = new ReporterBackedMapReduceDataWriterTaskTracker(Reporter.NULL);

    if (invalid) {
      Assert.expectThrows(VeniceException.class, () -> new KafkaInputRecordReader(split, conf, taskTracker));
      Assert.assertEquals(KafkaInputUtilsTest.RecordingPubSubConsumerAdapterFactory.getCreateCount(), 0);
      Assert.assertEquals(KafkaInputUtilsTest.RecordingPubSubConsumerAdapterFactory.getPollCount(), 0);
      return;
    }
    try (KafkaInputRecordReader reader = new KafkaInputRecordReader(split, conf, taskTracker)) {
      Function<String, String> observedLookup =
          KafkaInputUtilsTest.RecordingPubSubConsumerAdapterFactory.getObservedEncryptionKeyUrnLookup();
      if (Boolean.TRUE.equals(enabled)) {
        Assert.assertNotNull(observedLookup);
        Assert.assertEquals(observedLookup.apply("any-store"), keyUrn.trim());
      } else {
        Assert.assertNull(observedLookup);
      }
      Assert.assertEquals(KafkaInputUtilsTest.RecordingPubSubConsumerAdapterFactory.getCreateCount(), 1);
    }
  }
}
