package com.linkedin.venice.pubsub.adapter.kafka.consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
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
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubSymbolicPosition;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.api.exceptions.PubSubClientException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubClientRetriableException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubOpTimeoutException;
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
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
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
  private final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
  private final PubSubTopicPartition pubSubTopicPartition =
      new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic("test"), 0);
  private final TopicPartition topicPartition = new TopicPartition("test", 0);
  private Consumer<byte[], byte[]> internalKafkaConsumer;
  private ApacheKafkaConsumerAdapter kafkaConsumerAdapter;
  private PubSubMessageDeserializer pubSubMessageDeserializer;
  private TopicPartitionsOffsetsTracker topicPartitionsOffsetsTracker;
  private ApacheKafkaConsumerConfig apacheKafkaConsumerConfig;

  @BeforeMethod(alwaysRun = true)
  public void setUp() {
    internalKafkaConsumer = mock(Consumer.class);
    pubSubMessageDeserializer = PubSubMessageDeserializer.getInstance();
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
  public void testSubscribeWithValidOffset() {
    when(internalKafkaConsumer.assignment()).thenReturn(Collections.emptySet());
    doNothing().when(internalKafkaConsumer).assign(any());

    kafkaConsumerAdapter.subscribe(pubSubTopicPartition, 100);
    assertTrue(kafkaConsumerAdapter.getAssignment().contains(pubSubTopicPartition));
    verify(internalKafkaConsumer).assign(any(List.class));
    verify(internalKafkaConsumer).seek(topicPartition, 101); // Should seek to offset + 1

    kafkaConsumerAdapter.subscribe(pubSubTopicPartition, 200);
    verify(internalKafkaConsumer, times(2)).assign(any(List.class));
    verify(internalKafkaConsumer).seek(topicPartition, 201); // Should seek to offset + 1
  }

  @Test
  public void testSubscribeWithEarliestOffset() {
    when(internalKafkaConsumer.assignment()).thenReturn(Collections.emptySet());
    doNothing().when(internalKafkaConsumer).assign(any());

    kafkaConsumerAdapter.subscribe(pubSubTopicPartition, PubSubSymbolicPosition.EARLIEST);
    assertTrue(kafkaConsumerAdapter.getAssignment().contains(pubSubTopicPartition));
    verify(internalKafkaConsumer).assign(any(List.class));
    verify(internalKafkaConsumer).seekToBeginning(Collections.singletonList(topicPartition));

    kafkaConsumerAdapter.subscribe(pubSubTopicPartition, -1);
    verify(internalKafkaConsumer, times(2)).assign(any(List.class));
    verify(internalKafkaConsumer, times(2)).seekToBeginning(Collections.singletonList(topicPartition));
  }

  @Test
  public void testSubscribeAlreadySubscribed() {
    when(internalKafkaConsumer.assignment()).thenReturn(Collections.singleton(topicPartition));

    kafkaConsumerAdapter.subscribe(pubSubTopicPartition, 100);

    verify(internalKafkaConsumer, never()).assign(any());
    verify(internalKafkaConsumer, never()).seek(any(), anyLong());
  }

  @Test
  public void testSubscribeWithLatestPubSubPosition() {
    when(internalKafkaConsumer.assignment()).thenReturn(Collections.emptySet());

    kafkaConsumerAdapter.subscribe(pubSubTopicPartition, PubSubSymbolicPosition.LATEST);

    assertTrue(kafkaConsumerAdapter.getAssignment().contains(pubSubTopicPartition));
    verify(internalKafkaConsumer).assign(any(List.class));
    verify(internalKafkaConsumer).seekToEnd(Collections.singletonList(topicPartition));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testSubscribeWithNullPubSubPosition() {
    kafkaConsumerAdapter.subscribe(pubSubTopicPartition, null);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testSubscribeWithInvalidPubSubPositionType() {
    kafkaConsumerAdapter.subscribe(pubSubTopicPartition, mock(PubSubPosition.class));
  }

  @Test
  public void testSubscribeWithApacheKafkaOffsetPosition() {
    ApacheKafkaOffsetPosition offsetPosition = ApacheKafkaOffsetPosition.of(50);
    when(internalKafkaConsumer.assignment()).thenReturn(Collections.emptySet());

    kafkaConsumerAdapter.subscribe(pubSubTopicPartition, offsetPosition);
    assertTrue(kafkaConsumerAdapter.getAssignment().contains(pubSubTopicPartition));
    verify(internalKafkaConsumer).assign(any(List.class));
    verify(internalKafkaConsumer).seek(topicPartition, 51);
  }

  @Test
  public void testSubscribeTwiceWithSamePartition() {
    when(internalKafkaConsumer.assignment()).thenReturn(Collections.emptySet())
        .thenReturn(Collections.singleton(topicPartition));

    kafkaConsumerAdapter.subscribe(pubSubTopicPartition, 100);
    assertTrue(kafkaConsumerAdapter.getAssignment().contains(pubSubTopicPartition));
    kafkaConsumerAdapter.subscribe(pubSubTopicPartition, 200);
    verify(internalKafkaConsumer, times(1)).assign(any());
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
    Map<PubSubTopicPartition, List<DefaultPubSubMessage>> messages = kafkaConsumerAdapter.poll(Long.MAX_VALUE);

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

  @Test
  public void testBeginningOffsetSuccess() {
    Long expectedOffset = 0L;
    when(internalKafkaConsumer.beginningOffsets(Collections.singleton(topicPartition), Duration.ofMillis(500)))
        .thenReturn(Collections.singletonMap(topicPartition, expectedOffset));

    Long actualOffset = kafkaConsumerAdapter.beginningOffset(pubSubTopicPartition, Duration.ofMillis(500));
    assertEquals(actualOffset, expectedOffset);
  }

  @Test(expectedExceptions = PubSubOpTimeoutException.class)
  public void testBeginningOffsetThrowsTimeoutException() {
    when(internalKafkaConsumer.beginningOffsets(Collections.singleton(topicPartition), Duration.ofMillis(500)))
        .thenThrow(new TimeoutException("Test timeout"));

    kafkaConsumerAdapter.beginningOffset(pubSubTopicPartition, Duration.ofMillis(500));
  }

  @Test
  public void testBeginningPosition() {
    Map<TopicPartition, Long> mockResponse = Collections.singletonMap(topicPartition, 0L);
    doReturn(mockResponse).when(internalKafkaConsumer)
        .beginningOffsets(Collections.singleton(topicPartition), Duration.ofMillis(500));

    PubSubPosition position = kafkaConsumerAdapter.beginningPosition(pubSubTopicPartition, Duration.ofMillis(500));
    assertNotNull(position);
    assertTrue(position instanceof ApacheKafkaOffsetPosition);
    assertEquals(((ApacheKafkaOffsetPosition) position).getOffset(), 0L);

    // Case 2: return empty response
    doReturn(Collections.emptyMap()).when(internalKafkaConsumer)
        .beginningOffsets(Collections.singleton(topicPartition), Duration.ofMillis(500));
    position = kafkaConsumerAdapter.beginningPosition(pubSubTopicPartition, Duration.ofMillis(500));
    assertEquals(position, PubSubSymbolicPosition.EARLIEST);
  }

  @Test
  public void testEndOffsets() {
    PubSubTopicPartition pubSubTopicPartition1 =
        new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic("test"), 0);
    PubSubTopicPartition pubSubTopicPartition2 =
        new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic("test"), 1);
    TopicPartition topicPartition1 = new TopicPartition("test", 0);
    TopicPartition topicPartition2 = new TopicPartition("test", 1);
    Map<TopicPartition, Long> mockResponse = new HashMap<>();
    mockResponse.put(topicPartition1, 500L);
    mockResponse.put(topicPartition2, 600L);

    when(
        internalKafkaConsumer
            .endOffsets(new HashSet<>(Arrays.asList(topicPartition1, topicPartition2)), Duration.ofMillis(500)))
                .thenReturn(mockResponse);

    Map<PubSubTopicPartition, Long> actualOffsets = kafkaConsumerAdapter
        .endOffsets(Arrays.asList(pubSubTopicPartition1, pubSubTopicPartition2), Duration.ofMillis(500));
    assertEquals(actualOffsets.get(pubSubTopicPartition1), Long.valueOf(500));
    assertEquals(actualOffsets.get(pubSubTopicPartition2), Long.valueOf(600));

    Map<PubSubTopicPartition, PubSubPosition> actualPositions = kafkaConsumerAdapter
        .endPositions(Arrays.asList(pubSubTopicPartition1, pubSubTopicPartition2), Duration.ofMillis(500));
    assertTrue(actualPositions.get(pubSubTopicPartition1) instanceof ApacheKafkaOffsetPosition);
    assertTrue(actualPositions.get(pubSubTopicPartition2) instanceof ApacheKafkaOffsetPosition);
    assertEquals(((ApacheKafkaOffsetPosition) actualPositions.get(pubSubTopicPartition1)).getOffset(), 500L);
    assertEquals(((ApacheKafkaOffsetPosition) actualPositions.get(pubSubTopicPartition2)).getOffset(), 600L);
  }

  @Test(expectedExceptions = PubSubOpTimeoutException.class)
  public void testEndOffsetsThrowsTimeoutException() {
    when(internalKafkaConsumer.endOffsets(Collections.singleton(topicPartition), Duration.ofMillis(500)))
        .thenThrow(new TimeoutException("Test timeout"));
    kafkaConsumerAdapter.endOffsets(Collections.singleton(pubSubTopicPartition), Duration.ofMillis(500));
  }

  @Test
  public void testEndPosition() {
    long expectedOffset = 700L;
    Map<TopicPartition, Long> mockResponse = Collections.singletonMap(topicPartition, expectedOffset);
    doReturn(mockResponse).when(internalKafkaConsumer)
        .endOffsets(eq(Collections.singleton(topicPartition)), any(Duration.class));

    PubSubPosition position = kafkaConsumerAdapter.endPosition(pubSubTopicPartition);
    assertNotNull(position);
    assertTrue(position instanceof ApacheKafkaOffsetPosition);
    assertEquals(((ApacheKafkaOffsetPosition) position).getOffset(), expectedOffset);

    // Case 2: return empty response
    doReturn(Collections.emptyMap()).when(internalKafkaConsumer)
        .endOffsets(eq(Collections.singleton(topicPartition)), any(Duration.class));
    position = kafkaConsumerAdapter.endPosition(pubSubTopicPartition);
    assertEquals(position, PubSubSymbolicPosition.LATEST);
  }

  @Test
  public void testOffsetForTimeWithTimeoutSuccess() {
    PubSubTopicPartition pubSubTopicPartition = new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic("test"), 0);
    TopicPartition topicPartition =
        new TopicPartition(pubSubTopicPartition.getTopicName(), pubSubTopicPartition.getPartitionNumber());
    long timestamp = 1000000L;
    Long expectedOffset = 500L;
    OffsetAndTimestamp offsetAndTimestamp = new OffsetAndTimestamp(expectedOffset, timestamp);
    Map<TopicPartition, OffsetAndTimestamp> mockResponse = Collections.singletonMap(topicPartition, offsetAndTimestamp);

    when(
        internalKafkaConsumer
            .offsetsForTimes(Collections.singletonMap(topicPartition, timestamp), Duration.ofMillis(500)))
                .thenReturn(mockResponse);

    Long actualOffset = kafkaConsumerAdapter.offsetForTime(pubSubTopicPartition, timestamp, Duration.ofMillis(500));
    assertEquals(actualOffset, expectedOffset);
  }

  @Test
  public void testOffsetForTimeWithTimeoutReturnsNull() {
    PubSubTopicPartition pubSubTopicPartition = new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic("test"), 0);
    TopicPartition topicPartition =
        new TopicPartition(pubSubTopicPartition.getTopicName(), pubSubTopicPartition.getPartitionNumber());
    long timestamp = 1000000L;
    Map<TopicPartition, OffsetAndTimestamp> mockResponse = Collections.emptyMap();

    when(
        internalKafkaConsumer
            .offsetsForTimes(Collections.singletonMap(topicPartition, timestamp), Duration.ofMillis(500)))
                .thenReturn(mockResponse);

    Long actualOffset = kafkaConsumerAdapter.offsetForTime(pubSubTopicPartition, timestamp, Duration.ofMillis(500));
    assertNull(actualOffset);
  }

  @Test(expectedExceptions = PubSubOpTimeoutException.class)
  public void testOffsetForTimeWithTimeoutThrowsTimeoutException() {
    PubSubTopicPartition pubSubTopicPartition = new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic("test"), 0);
    TopicPartition topicPartition =
        new TopicPartition(pubSubTopicPartition.getTopicName(), pubSubTopicPartition.getPartitionNumber());
    long timestamp = 1000000L;

    when(
        internalKafkaConsumer
            .offsetsForTimes(Collections.singletonMap(topicPartition, timestamp), Duration.ofMillis(500)))
                .thenThrow(new TimeoutException("Test timeout"));

    kafkaConsumerAdapter.offsetForTime(pubSubTopicPartition, timestamp, Duration.ofMillis(500));
  }

  @Test
  public void testOffsetForTimeWithoutTimeoutSuccess() {
    PubSubTopicPartition pubSubTopicPartition = new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic("test"), 0);
    TopicPartition topicPartition =
        new TopicPartition(pubSubTopicPartition.getTopicName(), pubSubTopicPartition.getPartitionNumber());
    long timestamp = 1000000L;
    Long expectedOffset = 500L;
    OffsetAndTimestamp offsetAndTimestamp = new OffsetAndTimestamp(expectedOffset, timestamp);
    Map<TopicPartition, OffsetAndTimestamp> mockResponse = Collections.singletonMap(topicPartition, offsetAndTimestamp);

    when(internalKafkaConsumer.offsetsForTimes(Collections.singletonMap(topicPartition, timestamp)))
        .thenReturn(mockResponse);

    Long actualOffset = kafkaConsumerAdapter.offsetForTime(pubSubTopicPartition, timestamp);
    assertEquals(actualOffset, expectedOffset);
  }

  @Test
  public void testGetPositionByTimestampWithTimeout() {
    PubSubTopicPartition pubSubTopicPartition = new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic("test"), 0);
    TopicPartition topicPartition =
        new TopicPartition(pubSubTopicPartition.getTopicName(), pubSubTopicPartition.getPartitionNumber());
    long timestamp = 1000000L;
    long expectedOffset = 500L;

    Map<TopicPartition, OffsetAndTimestamp> offsetsForTimesResponse =
        Collections.singletonMap(topicPartition, new OffsetAndTimestamp(expectedOffset, timestamp));

    doReturn(offsetsForTimesResponse).when(internalKafkaConsumer)
        .offsetsForTimes(Collections.singletonMap(topicPartition, timestamp), Duration.ofMillis(500));

    PubSubPosition position =
        kafkaConsumerAdapter.getPositionByTimestamp(pubSubTopicPartition, timestamp, Duration.ofMillis(500));
    assertNotNull(position);
    assertTrue(position instanceof ApacheKafkaOffsetPosition);
    assertEquals(((ApacheKafkaOffsetPosition) position).getOffset(), expectedOffset);
  }

  @Test
  public void testGetPositionByTimestampWithoutTimeout() {
    PubSubTopicPartition pubSubTopicPartition = new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic("test"), 0);
    TopicPartition topicPartition =
        new TopicPartition(pubSubTopicPartition.getTopicName(), pubSubTopicPartition.getPartitionNumber());
    long timestamp = 1000000L;
    long expectedOffset = 500L;
    Map<TopicPartition, OffsetAndTimestamp> offsetsForTimesResponse =
        Collections.singletonMap(topicPartition, new OffsetAndTimestamp(expectedOffset, timestamp));

    doReturn(offsetsForTimesResponse).when(internalKafkaConsumer)
        .offsetsForTimes(Collections.singletonMap(topicPartition, timestamp));

    PubSubPosition position = kafkaConsumerAdapter.getPositionByTimestamp(pubSubTopicPartition, timestamp);
    assertNotNull(position);
    assertTrue(position instanceof ApacheKafkaOffsetPosition);
    assertEquals(((ApacheKafkaOffsetPosition) position).getOffset(), expectedOffset);
  }

  @Test
  public void testGetPositionByTimestampReturnsNull() {
    PubSubTopicPartition pubSubTopicPartition = new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic("test"), 0);
    TopicPartition topicPartition =
        new TopicPartition(pubSubTopicPartition.getTopicName(), pubSubTopicPartition.getPartitionNumber());
    long timestamp = 1000000L;

    doReturn(Collections.emptyMap()).when(internalKafkaConsumer)
        .offsetsForTimes(Collections.singletonMap(topicPartition, timestamp));

    PubSubPosition position = kafkaConsumerAdapter.getPositionByTimestamp(pubSubTopicPartition, timestamp);
    assertNull(position);
  }

  @Test
  public void testGetPositionByTimestampThrowsException() {
    PubSubTopicPartition pubSubTopicPartition = new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic("test"), 0);
    TopicPartition topicPartition =
        new TopicPartition(pubSubTopicPartition.getTopicName(), pubSubTopicPartition.getPartitionNumber());
    long timestamp = 1000000L;

    doThrow(new RuntimeException("Simulate exception")).when(internalKafkaConsumer)
        .offsetsForTimes(Collections.singletonMap(topicPartition, timestamp));

    Exception e = expectThrows(
        PubSubClientException.class,
        () -> kafkaConsumerAdapter.getPositionByTimestamp(pubSubTopicPartition, timestamp));
    assertTrue(e.getMessage().contains("Failed to fetch offset for time"), "Actual message: " + e.getMessage());
  }
}
