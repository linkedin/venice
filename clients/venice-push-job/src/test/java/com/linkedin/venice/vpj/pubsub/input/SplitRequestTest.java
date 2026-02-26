package com.linkedin.venice.vpj.pubsub.input;

import static com.linkedin.venice.vpj.pubsub.input.SplitRequest.DEFAULT_MAX_SPLITS;
import static com.linkedin.venice.vpj.pubsub.input.SplitRequest.DEFAULT_RECORDS_PER_SPLIT;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.manager.TopicManager;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class SplitRequestTest {
  private PubSubTopicPartition partition;
  private TopicManager topicManager;
  private PubSubPosition startPos;
  private PubSubPosition endPos;

  @BeforeMethod
  public void setUp() {
    PubSubTopicRepository topicRepository = new PubSubTopicRepository();
    PubSubTopic topic = topicRepository.getTopic("test-topic");
    partition = new PubSubTopicPartitionImpl(topic, 0);
    topicManager = mock(TopicManager.class);
    startPos = ApacheKafkaOffsetPosition.of(0);
    endPos = ApacheKafkaOffsetPosition.of(1000);
  }

  @Test
  public void testBasicGettersAndSetters() {
    SplitRequest request = new SplitRequest.Builder().pubSubTopicPartition(partition)
        .topicManager(topicManager)
        .splitType(PartitionSplitStrategy.FIXED_RECORD_COUNT)
        .recordsPerSplit(1000L)
        .startPosition(startPos)
        .endPosition(endPos)
        .numberOfRecords(1000L)
        .build();

    assertEquals(request.getPubSubTopicPartition(), partition);
    assertEquals(request.getTopicManager(), topicManager);
    assertEquals(request.getSplitType(), PartitionSplitStrategy.FIXED_RECORD_COUNT);
    assertEquals(request.getRecordsPerSplit(), 1000L);
    assertEquals(request.getStartPosition(), startPos);
    assertEquals(request.getEndPosition(), endPos);
    assertEquals(request.getNumberOfRecords(), 1000L);
  }

  @Test
  public void testDefaultValues() {
    // Case 1: CAPPED_SPLIT_COUNT with null maxSplits
    SplitRequest cappedRequest = new SplitRequest.Builder().pubSubTopicPartition(partition)
        .topicManager(topicManager)
        .splitType(PartitionSplitStrategy.CAPPED_SPLIT_COUNT)
        .startPosition(startPos)
        .endPosition(endPos)
        .numberOfRecords(1000L)
        .build();
    assertEquals((int) cappedRequest.getMaxSplits(), DEFAULT_MAX_SPLITS);

    // Case 2: FIXED_RECORD_COUNT with null recordsPerSplit
    SplitRequest fixedRequest = new SplitRequest.Builder().pubSubTopicPartition(partition)
        .topicManager(topicManager)
        .splitType(PartitionSplitStrategy.FIXED_RECORD_COUNT)
        .startPosition(startPos)
        .endPosition(endPos)
        .numberOfRecords(1000L)
        .build();
    assertEquals(fixedRequest.getRecordsPerSplit(), DEFAULT_RECORDS_PER_SPLIT);

    // Case 3: No PartitionSplitStrategy specified
    SplitRequest defaultRequest = new SplitRequest.Builder().pubSubTopicPartition(partition)
        .topicManager(topicManager)
        .startPosition(startPos)
        .endPosition(endPos)
        .numberOfRecords(1000L)
        .build();
    assertEquals(defaultRequest.getSplitType(), PartitionSplitStrategy.FIXED_RECORD_COUNT);
  }

  @Test
  public void testInvalidInputsUseDefaults() {
    // Case 1: CAPPED_SPLIT_COUNT with invalid maxSplits
    SplitRequest cappedRequest = new SplitRequest.Builder().pubSubTopicPartition(partition)
        .topicManager(topicManager)
        .splitType(PartitionSplitStrategy.CAPPED_SPLIT_COUNT)
        .maxSplits(-1)
        .startPosition(startPos)
        .endPosition(endPos)
        .numberOfRecords(1000L)
        .build();
    assertEquals((int) cappedRequest.getMaxSplits(), DEFAULT_MAX_SPLITS);

    // Case 2: FIXED_RECORD_COUNT with invalid recordsPerSplit
    SplitRequest fixedRequest = new SplitRequest.Builder().pubSubTopicPartition(partition)
        .topicManager(topicManager)
        .splitType(PartitionSplitStrategy.FIXED_RECORD_COUNT)
        .recordsPerSplit(0L)
        .startPosition(startPos)
        .endPosition(endPos)
        .numberOfRecords(1000L)
        .build();
    assertEquals(fixedRequest.getRecordsPerSplit(), DEFAULT_RECORDS_PER_SPLIT);
  }

  @Test
  public void testWholePartitionSplitType() {
    SplitRequest request = new SplitRequest.Builder().pubSubTopicPartition(partition)
        .topicManager(topicManager)
        .splitType(PartitionSplitStrategy.SINGLE_SPLIT_PER_PARTITION)
        .startPosition(startPos)
        .endPosition(endPos)
        .numberOfRecords(1000L)
        .build();

    assertEquals(request.getSplitType(), PartitionSplitStrategy.SINGLE_SPLIT_PER_PARTITION);
    assertNotNull(request.getPubSubTopicPartition());
    assertNotNull(request.getTopicManager());
  }

  @Test
  public void testBuilderValidation() {
    // Case 1: Missing pubSubTopicPartition
    expectThrows(NullPointerException.class, () -> {
      new SplitRequest.Builder().topicManager(topicManager)
          .splitType(PartitionSplitStrategy.SINGLE_SPLIT_PER_PARTITION)
          .startPosition(startPos)
          .endPosition(endPos)
          .numberOfRecords(1000L)
          .build();
    });

    // Case 2: Missing topicManager
    expectThrows(NullPointerException.class, () -> {
      new SplitRequest.Builder().pubSubTopicPartition(partition)
          .splitType(PartitionSplitStrategy.SINGLE_SPLIT_PER_PARTITION)
          .startPosition(startPos)
          .endPosition(endPos)
          .numberOfRecords(1000L)
          .build();
    });

    // Case 3: Missing startPosition
    expectThrows(NullPointerException.class, () -> {
      new SplitRequest.Builder().pubSubTopicPartition(partition)
          .topicManager(topicManager)
          .splitType(PartitionSplitStrategy.SINGLE_SPLIT_PER_PARTITION)
          .endPosition(endPos)
          .numberOfRecords(1000L)
          .build();
    });

    // Case 4: Missing endPosition
    expectThrows(NullPointerException.class, () -> {
      new SplitRequest.Builder().pubSubTopicPartition(partition)
          .topicManager(topicManager)
          .splitType(PartitionSplitStrategy.SINGLE_SPLIT_PER_PARTITION)
          .startPosition(startPos)
          .numberOfRecords(1000L)
          .build();
    });
  }

  @Test
  public void testToString() {
    SplitRequest request = new SplitRequest.Builder().pubSubTopicPartition(partition)
        .topicManager(topicManager)
        .splitType(PartitionSplitStrategy.FIXED_RECORD_COUNT)
        .recordsPerSplit(500L)
        .startPosition(startPos)
        .endPosition(endPos)
        .numberOfRecords(1000L)
        .build();

    String toString = request.toString();
    assertNotNull(toString);
    assertEquals(toString.contains("SplitRequest{"), true);
    assertEquals(toString.contains("partitionSplitStrategy=FIXED_RECORD_COUNT"), true, "Got: " + toString);
  }
}
