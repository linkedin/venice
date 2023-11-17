package com.linkedin.venice.pubsub.adapter.kafka.consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

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
import com.linkedin.venice.pubsub.adapter.kafka.TopicPartitionsOffsetsTracker;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.api.exceptions.PubSubClientException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubClientRetriableException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubTopicAuthorizationException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubUnsubscribedTopicPartitionException;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.serialization.avro.KafkaValueSerializer;
import com.linkedin.venice.serialization.avro.OptimizedKafkaValueSerializer;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.TimeoutException;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ApacheKafkaConsumerAdapterTest {
  private Consumer<byte[], byte[]> internalKafkaConsumer;
  private ApacheKafkaConsumerAdapter kafkaConsumerAdapter;
  private PubSubMessageDeserializer pubSubMessageDeserializer;
  private PubSubTopicRepository pubSubTopicRepository;
  private TopicPartitionsOffsetsTracker topicPartitionsOffsetsTracker;
  private ApacheKafkaConsumerConfig apacheKafkaConsumerConfig;

  @BeforeMethod(alwaysRun = true)
  public void setUp() {
    internalKafkaConsumer = mock(Consumer.class);
    pubSubMessageDeserializer = PubSubMessageDeserializer.getInstance();
    pubSubTopicRepository = new PubSubTopicRepository();
    topicPartitionsOffsetsTracker = mock(TopicPartitionsOffsetsTracker.class);
    apacheKafkaConsumerConfig = new ApacheKafkaConsumerConfig(new VeniceProperties(new Properties()), "testConsumer");
    kafkaConsumerAdapter = new ApacheKafkaConsumerAdapter(
        internalKafkaConsumer,
        apacheKafkaConsumerConfig,
        pubSubMessageDeserializer,
        topicPartitionsOffsetsTracker);
  }

  @AfterMethod(alwaysRun = true)
  public void cleanUp() {
    pubSubMessageDeserializer.close();
  }

  @Test
  public void testBatchUnsubscribe() {
    Map<PubSubTopicPartition, TopicPartition> topicPartitionBeingUnsubscribedMap = new HashMap<>();
    topicPartitionBeingUnsubscribedMap
        .put(new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic("t0_v1"), 0), new TopicPartition("t0_v1", 0));
    topicPartitionBeingUnsubscribedMap
        .put(new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic("t0_v1"), 1), new TopicPartition("t0_v1", 1));
    topicPartitionBeingUnsubscribedMap
        .put(new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic("t1_v1"), 1), new TopicPartition("t1_v1", 1));
    topicPartitionBeingUnsubscribedMap
        .put(new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic("t2_v1"), 2), new TopicPartition("t2_v1", 2));
    Set<PubSubTopicPartition> pubSubTopicPartitionsToUnsubscribe = topicPartitionBeingUnsubscribedMap.keySet();

    Map<PubSubTopicPartition, TopicPartition> unaffectedTopicPartitionsMap = new HashMap<>();
    unaffectedTopicPartitionsMap.put(
        new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic("t0_v1"), 101),
        new TopicPartition("t0_v1", 101));
    unaffectedTopicPartitionsMap
        .put(new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic("t4_v1"), 4), new TopicPartition("t4_v1", 4));

    Set<TopicPartition> currentAssignedTopicPartitions = new HashSet<>();
    currentAssignedTopicPartitions.addAll(topicPartitionBeingUnsubscribedMap.values());
    currentAssignedTopicPartitions.addAll(unaffectedTopicPartitionsMap.values());

    when(internalKafkaConsumer.assignment()).thenReturn(currentAssignedTopicPartitions);

    Set<TopicPartition> capturedReassignments = new HashSet<>();
    doAnswer(invocation -> {
      capturedReassignments.addAll(invocation.getArgument(0));
      return null;
    }).when(internalKafkaConsumer).assign(any());

    kafkaConsumerAdapter.batchUnsubscribe(pubSubTopicPartitionsToUnsubscribe);

    // verify that the reassignment contains all the unaffected topic partitions
    assertEquals(
        capturedReassignments,
        new HashSet<>(unaffectedTopicPartitionsMap.values()),
        "all unaffected topic partitions should be reassigned");
    verify(internalKafkaConsumer).assign(eq(capturedReassignments));
    // verify that the offsets tracker is updated for all the topic partitions being unsubscribed
    for (TopicPartition topicPartition: topicPartitionBeingUnsubscribedMap.values()) {
      verify(topicPartitionsOffsetsTracker).removeTrackedOffsets(topicPartition);
    }
  }

  @Test
  public void testPartitionsForReturnsNull() {
    PubSubTopic topic = mock(PubSubTopic.class);
    when(topic.getName()).thenReturn("testTopic");
    when(internalKafkaConsumer.partitionsFor(topic.getName())).thenReturn(null);
    assertNull(kafkaConsumerAdapter.partitionsFor(topic));
  }

  @Test
  public void testPartitionsForThrowsException() {
    doThrow(new TimeoutException("test")).when(internalKafkaConsumer).partitionsFor(any());
    Throwable throwable = expectThrows(
        PubSubClientRetriableException.class,
        () -> kafkaConsumerAdapter.partitionsFor(mock(PubSubTopic.class)));
    assertTrue(
        throwable.getMessage().contains("Retriable exception thrown when attempting to get partitions for topic:"));

    doThrow(AuthorizationException.class).when(internalKafkaConsumer).partitionsFor(any());
    throwable = expectThrows(
        PubSubTopicAuthorizationException.class,
        () -> kafkaConsumerAdapter.partitionsFor(mock(PubSubTopic.class)));
    assertTrue(
        throwable.getMessage().contains("Authorization exception thrown when attempting to get partitions for topic:"));

    doThrow(AuthenticationException.class).when(internalKafkaConsumer).partitionsFor(any());
    throwable = expectThrows(
        PubSubTopicAuthorizationException.class,
        () -> kafkaConsumerAdapter.partitionsFor(mock(PubSubTopic.class)));
    assertTrue(
        throwable.getMessage().contains("Authorization exception thrown when attempting to get partitions for topic:"));

    doThrow(InterruptException.class).when(internalKafkaConsumer).partitionsFor(any());
    throwable =
        expectThrows(PubSubClientException.class, () -> kafkaConsumerAdapter.partitionsFor(mock(PubSubTopic.class)));
    assertTrue(throwable.getMessage().contains("Exception thrown when attempting to get partitions for topic:"));
  }

  @Test
  public void testPartitionsFor() {
    PubSubTopic topic = mock(PubSubTopic.class);
    when(topic.getName()).thenReturn("testTopic");
    List<PartitionInfo> partitionInfos = new ArrayList<>();
    partitionInfos.add(new PartitionInfo("testTopic", 0, null, new Node[4], new Node[0]));
    partitionInfos.add(new PartitionInfo("testTopic", 1, null, new Node[3], new Node[1]));
    partitionInfos.add(new PartitionInfo("otherTopic", 0, null, new Node[0], new Node[0]));
    when(internalKafkaConsumer.partitionsFor(topic.getName())).thenReturn(partitionInfos);

    List<PubSubTopicPartitionInfo> result = kafkaConsumerAdapter.partitionsFor(topic);

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
  public void testDeserializer() {
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
        0,
        75,
        keySerializer.serialize("test", key),
        valueSerializer.serialize("test", value));
    ConsumerRecords<byte[], byte[]> records = new ConsumerRecords<>(
        Collections.singletonMap(new TopicPartition("test", 0), Collections.singletonList(record)));
    doReturn(records).when(internalKafkaConsumer).poll(any());

    PubSubTopicPartition pubSubTopicPartition = new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic("test"), 0);

    Node[] nodes = new Node[1];
    List<PartitionInfo> partitionInfoList = new ArrayList<>();
    partitionInfoList.add(new PartitionInfo(pubSubTopicPartition.getTopicName(), 0, nodes[0], nodes, nodes));
    when(internalKafkaConsumer.partitionsFor(pubSubTopicPartition.getTopicName())).thenReturn(partitionInfoList);

    // add partition to assignments
    kafkaConsumerAdapter.subscribe(pubSubTopicPartition, -1);
    // poll
    Map<PubSubTopicPartition, List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> messages =
        kafkaConsumerAdapter.poll(Long.MAX_VALUE);

    // verify
    assertEquals(messages.size(), 1);
    assertEquals(messages.get(pubSubTopicPartition).size(), 1);
    KafkaKey actualKey = messages.get(pubSubTopicPartition).get(0).getKey();
    assertEquals(actualKey.getKeyHeaderByte(), key.getKeyHeaderByte());
    assertEquals(actualKey.getKey(), key.getKey());
    assertEquals(messages.get(pubSubTopicPartition).get(0).getValue(), value);
  }

  @Test(expectedExceptions = VeniceMessageException.class, expectedExceptionsMessageRegExp = ".*The only supported Magic Byte for this.*")
  public void testDeserializerFailsWhenValueFormatIsInvalid() {
    KafkaKey key = new KafkaKey(MessageType.PUT, "key".getBytes());
    KafkaKeySerializer keySerializer = new KafkaKeySerializer();
    ConsumerRecord<byte[], byte[]> record =
        new ConsumerRecord<>("test", 42, 75, keySerializer.serialize("test", key), "value".getBytes());
    ConsumerRecords<byte[], byte[]> records = new ConsumerRecords<>(
        Collections.singletonMap(new TopicPartition("test", 42), Collections.singletonList(record)));
    doReturn(records).when(internalKafkaConsumer).poll(any());
    kafkaConsumerAdapter.poll(Long.MAX_VALUE);
  }

  @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*Illegal key header byte.*")
  public void testDeserializerFailsWhenKeyFormatIsInvalid() {
    ConsumerRecords<byte[], byte[]> consumerRecords = new ConsumerRecords<>(
        Collections.singletonMap(
            new TopicPartition("test", 42),
            Collections.singletonList(new ConsumerRecord<>("test", 42, 75, "key".getBytes(), "value".getBytes()))));
    doReturn(consumerRecords).when(internalKafkaConsumer).poll(any());
    kafkaConsumerAdapter.poll(Long.MAX_VALUE);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testApacheKafkaConsumer(boolean enabledOffsetCollection) {
    if (!enabledOffsetCollection) {
      kafkaConsumerAdapter = new ApacheKafkaConsumerAdapter(
          internalKafkaConsumer,
          apacheKafkaConsumerConfig,
          pubSubMessageDeserializer,
          null);
    }

    PubSubTopic testTopic = pubSubTopicRepository.getTopic("test_topic_v1");
    PubSubTopicPartition pubSubTopicPartition = new PubSubTopicPartitionImpl(testTopic, 1);
    TopicPartition topicPartition = new TopicPartition(testTopic.getName(), pubSubTopicPartition.getPartitionNumber());
    Assert.assertThrows(
        PubSubUnsubscribedTopicPartitionException.class,
        () -> kafkaConsumerAdapter.resetOffset(pubSubTopicPartition));

    List<PartitionInfo> partitionInfos = Arrays.asList(Mockito.mock(PartitionInfo.class));
    when(internalKafkaConsumer.partitionsFor(pubSubTopicPartition.getTopicName())).thenReturn(partitionInfos);

    Node[] nodes = new Node[1];
    List<PartitionInfo> partitionInfoList = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      partitionInfoList.add(new PartitionInfo(testTopic.getName(), i, nodes[0], nodes, nodes));
    }
    when(internalKafkaConsumer.partitionsFor(testTopic.getName())).thenReturn(partitionInfoList);

    // Test subscribe
    kafkaConsumerAdapter.subscribe(pubSubTopicPartition, OffsetRecord.LOWEST_OFFSET);
    verify(internalKafkaConsumer).assign(Collections.singletonList(topicPartition));
    verify(internalKafkaConsumer).seekToBeginning(Collections.singletonList(topicPartition));

    // Test assignment check.
    doReturn(Collections.singleton(topicPartition)).when(internalKafkaConsumer).assignment();
    assertTrue(kafkaConsumerAdapter.hasAnySubscription());

    // Test pause and resume
    kafkaConsumerAdapter.pause(pubSubTopicPartition);
    verify(internalKafkaConsumer).pause(Collections.singletonList(topicPartition));
    kafkaConsumerAdapter.resume(pubSubTopicPartition);
    verify(internalKafkaConsumer).resume(Collections.singletonList(topicPartition));

    // Test reset offset
    kafkaConsumerAdapter.resetOffset(pubSubTopicPartition);
    verify(internalKafkaConsumer, times(2)).seekToBeginning(Collections.singletonList(topicPartition));

    // Test unsubscribe
    kafkaConsumerAdapter.unSubscribe(pubSubTopicPartition);
    verify(internalKafkaConsumer).assign(Collections.EMPTY_LIST);

    // Test subscribe with not seek beginning.
    int lastReadOffset = 0;
    doReturn(Collections.EMPTY_SET).when(internalKafkaConsumer).assignment();
    assertFalse(kafkaConsumerAdapter.hasAnySubscription());
    kafkaConsumerAdapter.subscribe(pubSubTopicPartition, lastReadOffset);
    verify(internalKafkaConsumer).seek(topicPartition, lastReadOffset + 1);

    // Test batch unsubscribe.
    Set<PubSubTopicPartition> pubSubTopicPartitionsToUnSub = new HashSet<>();
    Set<TopicPartition> topicPartitionsLeft = new HashSet<>();
    Set<PubSubTopicPartition> allPubSubTopicPartitions = new HashSet<>();
    Set<TopicPartition> allTopicPartitions = new HashSet<>();
    for (int i = 0; i < 5; i++) {
      PubSubTopicPartition pubSubTopicPartitionToSub = new PubSubTopicPartitionImpl(testTopic, i);
      pubSubTopicPartitionsToUnSub.add(pubSubTopicPartitionToSub);
      allTopicPartitions.add(new TopicPartition(testTopic.getName(), i));
      allPubSubTopicPartitions.add(pubSubTopicPartitionToSub);
      kafkaConsumerAdapter.subscribe(pubSubTopicPartitionToSub, -1);
    }

    PubSubTopic testTopicV2 = pubSubTopicRepository.getTopic("test_topic_v2");

    partitionInfoList = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      partitionInfoList.add(new PartitionInfo(testTopicV2.getName(), i, nodes[0], nodes, nodes));
    }
    when(internalKafkaConsumer.partitionsFor(testTopicV2.getName())).thenReturn(partitionInfoList);

    for (int i = 0; i < 3; i++) {
      PubSubTopicPartition pubSubTopicPartitionToUnSub = new PubSubTopicPartitionImpl(testTopicV2, i);
      TopicPartition topicPartitionLeft = new TopicPartition(testTopicV2.getName(), i);
      topicPartitionsLeft.add(topicPartitionLeft);
      allTopicPartitions.add(topicPartitionLeft);
      allPubSubTopicPartitions.add(pubSubTopicPartitionToUnSub);
      kafkaConsumerAdapter.subscribe(pubSubTopicPartitionToUnSub, -1);
    }
    doReturn(allTopicPartitions).when(internalKafkaConsumer).assignment();
    assertTrue(kafkaConsumerAdapter.hasSubscription(pubSubTopicPartition));
    assertEquals(kafkaConsumerAdapter.getAssignment(), allPubSubTopicPartitions);
    kafkaConsumerAdapter.batchUnsubscribe(pubSubTopicPartitionsToUnSub);
    verify(internalKafkaConsumer).assign(topicPartitionsLeft);

    // Test close
    kafkaConsumerAdapter.close();
    verify(internalKafkaConsumer).close(eq(Duration.ZERO));
  }

  // isValidTopicPartition
  @Test
  public void testIsValidTopicPartitionReturnsFalseWhenRetriableExceptionIsThrown() {
    PubSubTopicPartition pubSubTopicPartition = new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic("test"), 0);
    int retryTimes = apacheKafkaConsumerConfig.getTopicQueryRetryTimes();
    doThrow(new TimeoutException()).when(internalKafkaConsumer).partitionsFor(pubSubTopicPartition.getTopicName());
    assertFalse(kafkaConsumerAdapter.isValidTopicPartition(pubSubTopicPartition));
    verify(internalKafkaConsumer, times(retryTimes)).partitionsFor(pubSubTopicPartition.getTopicName());
  }

  @Test
  public void testIsValidTopicPartition() {
    List<PartitionInfo> partitionInfos = new ArrayList<>();
    partitionInfos.add(new PartitionInfo("testTopic", 0, null, new Node[4], new Node[0]));
    PubSubTopicPartition pubSubTopicPartition =
        new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic("testTopic"), 0);
    doReturn(partitionInfos).when(internalKafkaConsumer).partitionsFor(pubSubTopicPartition.getTopicName());
    assertTrue(kafkaConsumerAdapter.isValidTopicPartition(pubSubTopicPartition));
    verify(internalKafkaConsumer).partitionsFor(pubSubTopicPartition.getTopicName());
  }
}
