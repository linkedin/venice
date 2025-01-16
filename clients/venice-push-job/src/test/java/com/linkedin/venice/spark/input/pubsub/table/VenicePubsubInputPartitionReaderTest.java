package com.linkedin.venice.spark.input.pubsub.table;

import static com.linkedin.venice.kafka.protocol.enums.MessageType.PUT;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_BROKER_URL;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_TOPIC;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_SOURCE_KEY_SCHEMA_STRING_PROP;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

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
import com.linkedin.venice.utils.VeniceProperties;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.spark.sql.catalyst.InternalRow;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class VenicePubsubInputPartitionReaderTest {
  private static final String KAFKA_MESSAGE_KEY_PREFIX = "key_";
  private static final String KAFKA_MESSAGE_VALUE_PREFIX = "value_";
  private static final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
  private VenicePubsubInputPartitionReader reader;
  private VenicePubsubInputPartition inputPartition;

  @BeforeMethod
  public void setUp() {
    Properties jobConfig = new Properties();
    int startingOffset = 0; // starting offset other than 0 needs mocking of subscription ...
    int endingOffset = 77; // total of 78 records
    int targetPartitionNumber = 42;

    String topicName = "BigStrangePubSubTopic_V1_rt_r";

    jobConfig.put(KAFKA_INPUT_BROKER_URL, "kafkaAddress");
    jobConfig.put(KAFKA_SOURCE_KEY_SCHEMA_STRING_PROP, ChunkedKeySuffix.SCHEMA$.toString());
    jobConfig.put(KAFKA_INPUT_TOPIC, topicName);

    PubSubConsumerAdapter consumer = mock(PubSubConsumerAdapter.class);

    long numRecords = endingOffset - startingOffset + 1;
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

    reader = new VenicePubsubInputPartitionReader(
        new VeniceProperties(jobConfig),
        inputPartition,
        consumer,
        pubSubTopicRepository);
  }

  /*
   * Test method for {@link com.linkedin.venice.spark.input.pubsub.table.VenicePubsubInputPartitionReader#next()}.
   * Runs through the records once and tests if the next() works correctly for both presence and absence of recrods.
   * Also ensures that the stats are recording the stats correctly.
   */
  @Test
  public void testNextAndGetStats() {

    System.out.println();
    assertTrue(reader.next()); // first record 0

    for (int i = 0; i < 10; i++) { // 10 gets just to break the balance
      reader.get();
    }

    for (int i = 1; i < 77; i++) { // 78 records expected
      reader.get();
      assertTrue(reader.next());
    }

    // skipped is zero in these cases , no control message in data
    // currentOffset, recordConvertedToRow, recordsSkipped, recordsDeliveredByGet
    List<Long> stats = reader.getStats();
    // since the offset starts at 0 for this test, we expect the offset to be equal to sum of skipped and converted
    assertEquals((long) stats.get(0), (long) stats.get(1) + stats.get(2));

    assertEquals(Arrays.asList(77L, 77L, 0L, 86L), reader.getStats());

    assertFalse(reader.next());
    assertEquals(Arrays.asList(77L, 77L, 0L, 86L), reader.getStats());
    reader.close();
  }

  @Test
  public void testGet() {
    for (int i = 0; i < 77; i++) { // 78 records expected
      InternalRow row = reader.get();
      long offset = row.getLong(0);
      byte[] key = row.getBinary(1);
      byte[] value = row.getBinary(2);

      assertEquals(key, (KAFKA_MESSAGE_KEY_PREFIX + offset).getBytes());
      assertEquals(value, (KAFKA_MESSAGE_VALUE_PREFIX + offset).getBytes());
    }
  }
}
