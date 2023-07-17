package com.linkedin.venice.kafka.consumer;

import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.exceptions.UnsubscribedTopicPartitionException;
import com.linkedin.venice.exceptions.VeniceMessageException;
import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicPartitionInfo;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.adapter.kafka.consumer.ApacheKafkaConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.serialization.avro.KafkaValueSerializer;
import com.linkedin.venice.serialization.avro.OptimizedKafkaValueSerializer;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.pools.LandFillObjectPool;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link ApacheKafkaConsumerAdapter}.
 */
public class ApacheKafkaPubSubConsumerAdapterTest {
  private ApacheKafkaConsumerAdapter apacheKafkaConsumerWithOffsetTrackingDisabled;
  private ApacheKafkaConsumerAdapter apacheKafkaConsumerWithOffsetTrackingEnabled;

  private KafkaConsumer<byte[], byte[]> delegateKafkaConsumer;
  private final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();

  @BeforeMethod
  public void initConsumer() {
    delegateKafkaConsumer = mock(KafkaConsumer.class);
    Properties properties = new Properties();
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    properties.setProperty(KAFKA_BOOTSTRAP_SERVERS, "broker address");
    PubSubMessageDeserializer pubSubMessageDeserializer = mock(PubSubMessageDeserializer.class);
    apacheKafkaConsumerWithOffsetTrackingDisabled = new ApacheKafkaConsumerAdapter(
        delegateKafkaConsumer,
        new VeniceProperties(properties),
        false,
        pubSubMessageDeserializer);

    apacheKafkaConsumerWithOffsetTrackingEnabled = new ApacheKafkaConsumerAdapter(
        delegateKafkaConsumer,
        new VeniceProperties(properties),
        true,
        pubSubMessageDeserializer);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testApacheKafkaConsumer(boolean enabledOffsetCollection) {
    ApacheKafkaConsumerAdapter consumer = enabledOffsetCollection
        ? apacheKafkaConsumerWithOffsetTrackingEnabled
        : apacheKafkaConsumerWithOffsetTrackingDisabled;
    PubSubTopic testTopic = pubSubTopicRepository.getTopic("test_topic_v1");
    PubSubTopicPartition pubSubTopicPartition = new PubSubTopicPartitionImpl(testTopic, 1);
    TopicPartition topicPartition = new TopicPartition(testTopic.getName(), pubSubTopicPartition.getPartitionNumber());
    Assert.assertThrows(UnsubscribedTopicPartitionException.class, () -> consumer.resetOffset(pubSubTopicPartition));

    // Test subscribe
    consumer.subscribe(pubSubTopicPartition, OffsetRecord.LOWEST_OFFSET);
    verify(delegateKafkaConsumer).assign(Collections.singletonList(topicPartition));
    verify(delegateKafkaConsumer).seekToBeginning(Collections.singletonList(topicPartition));

    // Test assignment check.
    doReturn(Collections.singleton(topicPartition)).when(delegateKafkaConsumer).assignment();
    Assert.assertTrue(consumer.hasAnySubscription());

    // Test pause and resume
    consumer.pause(pubSubTopicPartition);
    verify(delegateKafkaConsumer).pause(Collections.singletonList(topicPartition));
    consumer.resume(pubSubTopicPartition);
    verify(delegateKafkaConsumer).resume(Collections.singletonList(topicPartition));

    // Test reset offset
    consumer.resetOffset(pubSubTopicPartition);
    verify(delegateKafkaConsumer, times(2)).seekToBeginning(Collections.singletonList(topicPartition));

    // Test unsubscribe
    consumer.unSubscribe(pubSubTopicPartition);
    verify(delegateKafkaConsumer).assign(Collections.EMPTY_LIST);

    // Test subscribe with not seek beginning.
    int lastReadOffset = 0;
    doReturn(Collections.EMPTY_SET).when(delegateKafkaConsumer).assignment();
    Assert.assertFalse(consumer.hasAnySubscription());
    consumer.subscribe(pubSubTopicPartition, lastReadOffset);
    verify(delegateKafkaConsumer).seek(topicPartition, lastReadOffset + 1);

    // Test batch unsubscribe.
    Set<PubSubTopicPartition> pubSubTopicPartitionsToUnSub = new HashSet<>();
    Set<TopicPartition> topicPartitionsLeft = new HashSet<>();
    Set<PubSubTopicPartition> allPubSubTopicPartitions = new HashSet<>();
    Set<TopicPartition> allTopicPartitions = new HashSet<>();
    PubSubTopic testTopicV2 = pubSubTopicRepository.getTopic("test_topic_v2");
    for (int i = 0; i < 5; i++) {
      PubSubTopicPartition pubSubTopicPartitionToSub = new PubSubTopicPartitionImpl(testTopic, i);
      pubSubTopicPartitionsToUnSub.add(pubSubTopicPartitionToSub);
      allTopicPartitions.add(new TopicPartition(testTopic.getName(), i));
      allPubSubTopicPartitions.add(pubSubTopicPartitionToSub);
      consumer.subscribe(pubSubTopicPartitionToSub, -1);
    }
    for (int i = 0; i < 3; i++) {
      PubSubTopicPartition pubSubTopicPartitionToUnSub = new PubSubTopicPartitionImpl(testTopicV2, i);
      TopicPartition topicPartitionLeft = new TopicPartition(testTopicV2.getName(), i);
      topicPartitionsLeft.add(topicPartitionLeft);
      allTopicPartitions.add(topicPartitionLeft);
      allPubSubTopicPartitions.add(pubSubTopicPartitionToUnSub);
      consumer.subscribe(pubSubTopicPartitionToUnSub, -1);
    }
    doReturn(allTopicPartitions).when(delegateKafkaConsumer).assignment();
    Assert.assertTrue(consumer.hasSubscription(pubSubTopicPartition));
    assertEquals(consumer.getAssignment(), allPubSubTopicPartitions);
    consumer.batchUnsubscribe(pubSubTopicPartitionsToUnSub);
    verify(delegateKafkaConsumer).assign(topicPartitionsLeft);

    // Test close
    consumer.close();
    verify(delegateKafkaConsumer).close(eq(Duration.ZERO));
  }

  @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*Illegal key header byte.*")
  public void testDeserializerFailsWhenKeyFormatIsInvalid() {
    Consumer<byte[], byte[]> consumer = mock(Consumer.class);
    PubSubMessageDeserializer pubSubMessageDeserializer = new PubSubMessageDeserializer(
        new OptimizedKafkaValueSerializer(),
        new LandFillObjectPool<>(KafkaMessageEnvelope::new),
        new LandFillObjectPool<>(KafkaMessageEnvelope::new));
    ConsumerRecords<byte[], byte[]> consumerRecords = new ConsumerRecords<>(
        Collections.singletonMap(
            new TopicPartition("test", 42),
            Collections.singletonList(new ConsumerRecord<>("test", 42, 75, "key".getBytes(), "value".getBytes()))));
    doReturn(consumerRecords).when(consumer).poll(any());
    new ApacheKafkaConsumerAdapter(consumer, new VeniceProperties(new Properties()), false, pubSubMessageDeserializer)
        .poll(Long.MAX_VALUE);
  }

  @Test(expectedExceptions = VeniceMessageException.class, expectedExceptionsMessageRegExp = ".*The only supported Magic Byte for this.*")
  public void testDeserializerFailsWhenValueFormatIsInvalid() {
    Consumer<byte[], byte[]> consumer = mock(Consumer.class);
    PubSubMessageDeserializer pubSubMessageDeserializer = new PubSubMessageDeserializer(
        new OptimizedKafkaValueSerializer(),
        new LandFillObjectPool<>(KafkaMessageEnvelope::new),
        new LandFillObjectPool<>(KafkaMessageEnvelope::new));
    KafkaKey key = new KafkaKey(MessageType.PUT, "key".getBytes());
    KafkaKeySerializer keySerializer = new KafkaKeySerializer();
    ConsumerRecord<byte[], byte[]> record =
        new ConsumerRecord<>("test", 42, 75, keySerializer.serialize("test", key), "value".getBytes());
    ConsumerRecords<byte[], byte[]> records = new ConsumerRecords<>(
        Collections.singletonMap(new TopicPartition("test", 42), Collections.singletonList(record)));
    doReturn(records).when(consumer).poll(any());
    new ApacheKafkaConsumerAdapter(consumer, new VeniceProperties(new Properties()), false, pubSubMessageDeserializer)
        .poll(Long.MAX_VALUE);
  }

  @Test
  public void testDeserializer() {
    Consumer<byte[], byte[]> consumer = mock(Consumer.class);
    PubSubMessageDeserializer pubSubMessageDeserializer = new PubSubMessageDeserializer(
        new OptimizedKafkaValueSerializer(),
        new LandFillObjectPool<>(KafkaMessageEnvelope::new),
        new LandFillObjectPool<>(KafkaMessageEnvelope::new));
    KafkaKey key = new KafkaKey(MessageType.PUT, "key".getBytes());
    KafkaKeySerializer keySerializer = new KafkaKeySerializer();

    KafkaMessageEnvelope value = new KafkaMessageEnvelope();
    value.producerMetadata = new ProducerMetadata();
    value.producerMetadata.messageTimestamp = 0;
    value.producerMetadata.messageSequenceNumber = 0;
    value.producerMetadata.segmentNumber = 0;
    value.producerMetadata.producerGUID = new GUID();
    Put put = new Put();
    put.putValue = ByteBuffer.allocate(1024);
    put.replicationMetadataPayload = ByteBuffer.allocate(0);
    value.payloadUnion = put;

    KafkaValueSerializer valueSerializer = new OptimizedKafkaValueSerializer();
    ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
        "test",
        42,
        75,
        keySerializer.serialize("test", key),
        valueSerializer.serialize("test", value));
    ConsumerRecords<byte[], byte[]> records = new ConsumerRecords<>(
        Collections.singletonMap(new TopicPartition("test", 42), Collections.singletonList(record)));
    doReturn(records).when(consumer).poll(any());

    ApacheKafkaConsumerAdapter consumerAdapter = new ApacheKafkaConsumerAdapter(
        consumer,
        new VeniceProperties(new Properties()),
        false,
        pubSubMessageDeserializer);
    PubSubTopicPartition pubSubTopicPartition =
        new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic("test"), 42);
    // add partition to assignments
    consumerAdapter.subscribe(pubSubTopicPartition, -1);
    // poll
    Map<PubSubTopicPartition, List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> messages =
        consumerAdapter.poll(Long.MAX_VALUE);

    // verify
    assertEquals(messages.size(), 1);
    assertEquals(messages.get(pubSubTopicPartition).size(), 1);
    KafkaKey actualKey = messages.get(pubSubTopicPartition).get(0).getKey();
    assertEquals(actualKey.getKeyHeaderByte(), key.getKeyHeaderByte());
    assertEquals(actualKey.getKey(), key.getKey());
    assertEquals(messages.get(pubSubTopicPartition).get(0).getValue(), value);
  }

  @Test
  public void testPartitionsFor() {
    PubSubTopic topic = mock(PubSubTopic.class);
    when(topic.getName()).thenReturn("testTopic");
    List<PartitionInfo> partitionInfos = new ArrayList<>();
    partitionInfos.add(new PartitionInfo("testTopic", 0, null, new Node[4], new Node[0]));
    partitionInfos.add(new PartitionInfo("testTopic", 1, null, new Node[3], new Node[1]));
    partitionInfos.add(new PartitionInfo("otherTopic", 0, null, new Node[0], new Node[0]));
    when(delegateKafkaConsumer.partitionsFor(topic.getName())).thenReturn(partitionInfos);

    List<PubSubTopicPartitionInfo> result = apacheKafkaConsumerWithOffsetTrackingDisabled.partitionsFor(topic);

    assertNotNull(result);
    assertEquals(result.size(), 2);

    PubSubTopicPartitionInfo topicPartitionInfo = result.get(0);
    assertEquals(topicPartitionInfo.topic(), topic);
    assertEquals(topicPartitionInfo.topic().getName(), "testTopic");
    assertEquals(topicPartitionInfo.partition(), 0);
    assertFalse(topicPartitionInfo.hasInSyncReplicas());

    topicPartitionInfo = result.get(1);
    assertEquals(topicPartitionInfo.topic(), topic);
    assertEquals(topicPartitionInfo.topic().getName(), "testTopic");
    assertTrue(topicPartitionInfo.hasInSyncReplicas());
  }

  @Test
  public void testPartitionsForReturnsNull() {
    PubSubTopic topic = mock(PubSubTopic.class);
    when(topic.getName()).thenReturn("testTopic");
    when(delegateKafkaConsumer.partitionsFor(topic.getName())).thenReturn(null);
    assertNull(apacheKafkaConsumerWithOffsetTrackingDisabled.partitionsFor(topic));
  }
}
