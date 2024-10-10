package com.linkedin.venice.spark.input.pubsub.table;

import static com.linkedin.venice.kafka.protocol.enums.MessageType.*;
import static com.linkedin.venice.vpj.VenicePushJobConstants.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.ImmutablePubSubMessage;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.storage.protocol.ChunkedKeySuffix;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.spark.sql.catalyst.InternalRow;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


public class VenicePubsubInputPartitionReaderTest {
  private static final String KAFKA_MESSAGE_KEY_PREFIX = "key_";
  private static final String KAFKA_MESSAGE_VALUE_PREFIX = "value_";
  private static final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
  private VenicePubsubInputPartitionReader reader;
  private VenicePubsubInputPartition inputPartition;

  @BeforeTest
  public void setUp() {
    Properties jobConfig = new Properties();
    int startingOffset = 0; // starting offset other than 0 needs mocking of subscription ...
    int endingOffset = 100;
    int targetPartitionNumber = 42;

    String topicName = "BigStrangePubSubTopic_V1_rt_r";

    jobConfig.put(KAFKA_INPUT_BROKER_URL, "kafkaAddress");
    jobConfig.put(KAFKA_SOURCE_KEY_SCHEMA_STRING_PROP, ChunkedKeySuffix.SCHEMA$.toString());
    jobConfig.put(KAFKA_INPUT_TOPIC, topicName);

    PubSubConsumerAdapter consumer = mock(PubSubConsumerAdapter.class);

    long numRecords = endingOffset - startingOffset;
    List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>> consumerRecordList = new ArrayList<>();

    PubSubTopicPartition pubSubTopicPartition =
        new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic(topicName), targetPartitionNumber);

    // fill the topic message array
    for (int i = startingOffset; i < numRecords; ++i) {
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
      put.schemaId = 42; // shouldn't go with -1, -1 is reserved for chunking .
      put.putValue = ByteBuffer.wrap(valueBytes);
      put.replicationMetadataPayload = ByteBuffer.allocate(0);
      messageEnvelope.payloadUnion = put;
      consumerRecordList.add(new ImmutablePubSubMessage<>(kafkaKey, messageEnvelope, pubSubTopicPartition, i, -1, -1));
    }

    Map<PubSubTopicPartition, List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> recordsMap = new HashMap<>();
    recordsMap.put(pubSubTopicPartition, consumerRecordList);
    when(consumer.poll(anyLong())).thenReturn(recordsMap, new HashMap<>());

    inputPartition =
        new VenicePubsubInputPartition("prod-lva2", topicName, targetPartitionNumber, startingOffset, endingOffset);

    reader = new VenicePubsubInputPartitionReader(jobConfig, inputPartition, consumer, pubSubTopicRepository);
  }

  @Test
  public void testNext() {
    assertTrue(reader.next());
    for (int i = 0; i < 99; i++) {
      reader.get();
      assertTrue(reader.next());
    }
    reader.get();
    assertFalse(reader.next());
    reader.close();
  }

  @Test
  public void testGet() {
    InternalRow row = reader.get();
    System.out.println(row);
    long offset = row.getLong(0);
    byte[] key = row.getBinary(1);
    byte[] value = row.getBinary(2);

    assertEquals(key, (KAFKA_MESSAGE_KEY_PREFIX + offset).getBytes());
    assertEquals(value, (KAFKA_MESSAGE_VALUE_PREFIX + offset).getBytes());

    // assertEquals(row.get(0, KafkaKey), "dummyData1");
    // assertTrue(row.getInt(1) >= 0);
    // assertTrue(row.getInt(1) < 1000);
    // assertTrue(row.getBoolean(2));
  }

  @Test
  public void testClose() {
    reader.close();
    // Add assertions if there are any resources to verify after close
  }
}
