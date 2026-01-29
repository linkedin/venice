package com.linkedin.venice.vpj.pubsub.input;

import static com.linkedin.venice.vpj.pubsub.input.SplitRequest.DEFAULT_MAX_SPLITS;
import static com.linkedin.venice.vpj.pubsub.input.SplitRequest.DEFAULT_RECORDS_PER_SPLIT;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.manager.TopicManager;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class SplitRequestTest {
  private PubSubTopicPartition partition;
  private TopicManager topicManager;

  @BeforeMethod
  public void setUp() {
    PubSubTopicRepository topicRepository = new PubSubTopicRepository();
    PubSubTopic topic = topicRepository.getTopic("test-topic");
    partition = new PubSubTopicPartitionImpl(topic, 0);
    topicManager = mock(TopicManager.class);
  }

  @Test
  public void testBasicGettersAndSetters() {
    SplitRequest request = new SplitRequest.Builder().pubSubTopicPartition(partition)
        .topicManager(topicManager)
        .splitType(PartitionSplitStrategy.FIXED_RECORD_COUNT)
        .recordsPerSplit(1000L)
        .build();

    assertEquals(request.getPubSubTopicPartition(), partition);
    assertEquals(request.getTopicManager(), topicManager);
    assertEquals(request.getSplitType(), PartitionSplitStrategy.FIXED_RECORD_COUNT);
    assertEquals(request.getRecordsPerSplit(), 1000L);
  }

  @Test
  public void testDefaultValues() {
    // Case 1: CAPPED_SPLIT_COUNT with null maxSplits
    SplitRequest cappedRequest = new SplitRequest.Builder().pubSubTopicPartition(partition)
        .topicManager(topicManager)
        .splitType(PartitionSplitStrategy.CAPPED_SPLIT_COUNT)
        .build();
    assertEquals((int) cappedRequest.getMaxSplits(), DEFAULT_MAX_SPLITS);

    // Case 2: FIXED_RECORD_COUNT with null recordsPerSplit
    SplitRequest fixedRequest = new SplitRequest.Builder().pubSubTopicPartition(partition)
        .topicManager(topicManager)
        .splitType(PartitionSplitStrategy.FIXED_RECORD_COUNT)
        .build();
    assertEquals(fixedRequest.getRecordsPerSplit(), DEFAULT_RECORDS_PER_SPLIT);

    // Case 3: No PartitionSplitStrategy specified
    SplitRequest defaultRequest =
        new SplitRequest.Builder().pubSubTopicPartition(partition).topicManager(topicManager).build();
    assertEquals(defaultRequest.getSplitType(), PartitionSplitStrategy.FIXED_RECORD_COUNT);
  }

  @Test
  public void testInvalidInputsUseDefaults() {
    // Case 1: CAPPED_SPLIT_COUNT with invalid maxSplits
    SplitRequest cappedRequest = new SplitRequest.Builder().pubSubTopicPartition(partition)
        .topicManager(topicManager)
        .splitType(PartitionSplitStrategy.CAPPED_SPLIT_COUNT)
        .maxSplits(-1)
        .build();
    assertEquals((int) cappedRequest.getMaxSplits(), DEFAULT_MAX_SPLITS);

    // Case 2: FIXED_RECORD_COUNT with invalid recordsPerSplit
    SplitRequest fixedRequest = new SplitRequest.Builder().pubSubTopicPartition(partition)
        .topicManager(topicManager)
        .splitType(PartitionSplitStrategy.FIXED_RECORD_COUNT)
        .recordsPerSplit(0L)
        .build();
    assertEquals(fixedRequest.getRecordsPerSplit(), DEFAULT_RECORDS_PER_SPLIT);
  }

  @Test
  public void testWholePartitionSplitType() {
    SplitRequest request = new SplitRequest.Builder().pubSubTopicPartition(partition)
        .topicManager(topicManager)
        .splitType(PartitionSplitStrategy.SINGLE_SPLIT_PER_PARTITION)
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
          .build();
    });

    // Case 2: Missing topicManager
    expectThrows(NullPointerException.class, () -> {
      new SplitRequest.Builder().pubSubTopicPartition(partition)
          .splitType(PartitionSplitStrategy.SINGLE_SPLIT_PER_PARTITION)
          .build();
    });
  }

  @Test
  public void testToString() {
    SplitRequest request = new SplitRequest.Builder().pubSubTopicPartition(partition)
        .topicManager(topicManager)
        .splitType(PartitionSplitStrategy.FIXED_RECORD_COUNT)
        .recordsPerSplit(500L)
        .build();

    String toString = request.toString();
    assertNotNull(toString);
    assertEquals(toString.contains("SplitRequest{"), true);
    assertEquals(toString.contains("partitionSplitStrategy=FIXED_RECORD_COUNT"), true, "Got: " + toString);
  }
}
