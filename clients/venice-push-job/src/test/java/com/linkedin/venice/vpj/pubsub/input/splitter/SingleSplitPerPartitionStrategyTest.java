package com.linkedin.venice.vpj.pubsub.input.splitter;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.vpj.pubsub.input.PartitionSplitStrategy;
import com.linkedin.venice.vpj.pubsub.input.PubSubPartitionSplit;
import com.linkedin.venice.vpj.pubsub.input.SplitRequest;
import java.util.List;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link SingleSplitPerPartitionStrategy}.
 */
public class SingleSplitPerPartitionStrategyTest {
  private static final PubSubTopicRepository TOPIC_REPOSITORY = new PubSubTopicRepository();

  private SingleSplitPerPartitionStrategy splitter;
  private TopicManager topicManager;
  private PubSubTopic topic;
  private PubSubTopicPartition partition;

  @BeforeMethod
  public void setUp() {
    topic = TOPIC_REPOSITORY.getTopic("test-topic");
    partition = new PubSubTopicPartitionImpl(topic, 0);
    topicManager = mock(TopicManager.class);
    splitter = new SingleSplitPerPartitionStrategy();
  }

  @Test
  public void testNormalSplitScenarios() {
    // Case 1: Normal partition with records
    PubSubPosition startPos = ApacheKafkaOffsetPosition.of(100);
    PubSubPosition endPos = ApacheKafkaOffsetPosition.of(1000);
    long recordCount = 900;
    setupTopicManagerMocks(startPos, endPos, recordCount);

    SplitRequest request = createSplitRequest();
    List<PubSubPartitionSplit> splits = splitter.split(request);

    assertEquals(splits.size(), 1, "Should create exactly one split for whole partition");
    verifySingleSplit(splits.get(0), startPos, endPos, recordCount, 0);
    verifyTopicManagerMethodCalls();

    // Case 2: Large partition with many records
    reset(topicManager);
    startPos = ApacheKafkaOffsetPosition.of(0);
    endPos = ApacheKafkaOffsetPosition.of(1000000);
    recordCount = 1000000;
    setupTopicManagerMocks(startPos, endPos, recordCount);

    request = createSplitRequest();
    splits = splitter.split(request);

    assertEquals(splits.size(), 1, "Should create exactly one split for large partition");
    verifySingleSplit(splits.get(0), startPos, endPos, recordCount, 0);
    verifyTopicManagerMethodCalls();

    // Case 3: Small partition with few records
    reset(topicManager);
    startPos = ApacheKafkaOffsetPosition.of(50);
    endPos = ApacheKafkaOffsetPosition.of(55);
    recordCount = 5;
    setupTopicManagerMocks(startPos, endPos, recordCount);

    request = createSplitRequest();
    splits = splitter.split(request);

    assertEquals(splits.size(), 1, "Should create exactly one split for small partition");
    verifySingleSplit(splits.get(0), startPos, endPos, recordCount, 0);
    verifyTopicManagerMethodCalls();
  }

  @Test
  public void testEdgeCaseSplitScenarios() {
    // Case 1: Empty partition (no records)
    PubSubPosition startPos = ApacheKafkaOffsetPosition.of(100);
    PubSubPosition endPos = ApacheKafkaOffsetPosition.of(100);
    long recordCount = 0;
    setupTopicManagerMocks(startPos, endPos, recordCount);

    SplitRequest request = createSplitRequest();
    List<PubSubPartitionSplit> splits = splitter.split(request);

    assertEquals(splits.size(), 0, "Should create no splits for empty partition");
    verifyTopicManagerMethodCalls();

    // Case 2: Single record partition
    reset(topicManager);
    startPos = ApacheKafkaOffsetPosition.of(42);
    endPos = ApacheKafkaOffsetPosition.of(43);
    recordCount = 1;
    setupTopicManagerMocks(startPos, endPos, recordCount);

    request = createSplitRequest();
    splits = splitter.split(request);

    assertEquals(splits.size(), 1, "Should create exactly one split for single record partition");
    verifySingleSplit(splits.get(0), startPos, endPos, recordCount, 0);
    verifyTopicManagerMethodCalls();

    // Case 3: Partition with start position at zero
    reset(topicManager);
    startPos = ApacheKafkaOffsetPosition.of(0);
    endPos = ApacheKafkaOffsetPosition.of(1000);
    recordCount = 1000;
    setupTopicManagerMocks(startPos, endPos, recordCount);

    request = createSplitRequest();
    splits = splitter.split(request);

    assertEquals(splits.size(), 1, "Should create exactly one split for partition starting at zero");
    verifySingleSplit(splits.get(0), startPos, endPos, recordCount, 0);
    verifyTopicManagerMethodCalls();
  }

  @DataProvider(name = "partitionSizeScenarios")
  public Object[][] partitionSizeScenarios() {
    return new Object[][] {
        // {startOffset, endOffset, expectedRecordCount, description}
        { 0L, 1000L, 1000L, "Standard partition" }, { 100L, 200L, 100L, "Mid-range partition" },
        { 1000000L, 2000000L, 1000000L, "Large partition" }, { 42L, 43L, 1L, "Single record partition" } };
  }

  @Test(dataProvider = "partitionSizeScenarios")
  public void testVariousPartitionSizes(
      long startOffset,
      long endOffset,
      long expectedRecordCount,
      String description) {
    PubSubPosition startPos = ApacheKafkaOffsetPosition.of(startOffset);
    PubSubPosition endPos = ApacheKafkaOffsetPosition.of(endOffset);
    setupTopicManagerMocks(startPos, endPos, expectedRecordCount);

    SplitRequest request = createSplitRequest();
    List<PubSubPartitionSplit> splits = splitter.split(request);

    assertEquals(splits.size(), 1, "Should always create exactly one split: " + description);
    verifySingleSplit(splits.get(0), startPos, endPos, expectedRecordCount, 0);
    verifyTopicManagerMethodCalls();

    reset(topicManager);
  }

  @Test
  public void testSplitPropertiesAndStructure() {
    // Case 1: Verify split properties are correctly set
    PubSubPosition startPos = ApacheKafkaOffsetPosition.of(500);
    PubSubPosition endPos = ApacheKafkaOffsetPosition.of(1500);
    long recordCount = 1000;
    setupTopicManagerMocks(startPos, endPos, recordCount);

    SplitRequest request = createSplitRequest();
    List<PubSubPartitionSplit> splits = splitter.split(request);

    PubSubPartitionSplit split = splits.get(0);
    assertEquals(split.getPubSubTopicPartition(), partition, "Split should reference correct partition");
    assertEquals(split.getStartPubSubPosition(), startPos, "Split start position should match");
    assertEquals(split.getEndPubSubPosition(), endPos, "Split end position should match");
    assertEquals(split.getNumberOfRecords(), recordCount, "Split record count should match");
    assertEquals(split.getSplitIndex(), 0, "Split should always have range index 0");

    // Verify startOffset value directly from the split
    assertEquals(split.getStartIndex(), 0, "Split should have start offset 0 for whole partition");

    // Case 2: Verify immutability and consistency across multiple calls
    reset(topicManager);
    setupTopicManagerMocks(startPos, endPos, recordCount);

    List<PubSubPartitionSplit> secondSplits = splitter.split(request);
    assertEquals(secondSplits.size(), splits.size(), "Multiple calls should return same structure");
    assertEquals(
        secondSplits.get(0).getSplitIndex(),
        splits.get(0).getSplitIndex(),
        "Range index should be consistent");
  }

  // Helper methods

  private SplitRequest createSplitRequest() {
    return new SplitRequest.Builder().pubSubTopicPartition(partition)
        .topicManager(topicManager)
        .splitType(PartitionSplitStrategy.SINGLE_SPLIT_PER_PARTITION)
        .build();
  }

  private void setupTopicManagerMocks(PubSubPosition startPos, PubSubPosition endPos, long recordCount) {
    when(topicManager.getStartPositionsForPartitionWithRetries(partition)).thenReturn(startPos);
    when(topicManager.getEndPositionsForPartitionWithRetries(partition)).thenReturn(endPos);
    when(topicManager.diffPosition(partition, endPos, startPos)).thenReturn(recordCount);
    when(topicManager.getTopicRepository()).thenReturn(TOPIC_REPOSITORY);
  }

  private void verifySingleSplit(
      PubSubPartitionSplit split,
      PubSubPosition expectedStartPos,
      PubSubPosition expectedEndPos,
      long expectedRecordCount,
      int expectedRangeIndex) {
    assertNotNull(split, "Split should not be null");
    assertEquals(split.getPubSubTopicPartition(), partition, "Split partition should match request");
    assertEquals(split.getStartPubSubPosition(), expectedStartPos, "Split start position should match");
    assertEquals(split.getEndPubSubPosition(), expectedEndPos, "Split end position should match");
    assertEquals(split.getNumberOfRecords(), expectedRecordCount, "Split record count should match");
    assertEquals(split.getSplitIndex(), expectedRangeIndex, "Split range index should match");

    // Verify startOffset value directly from the split
    assertEquals(split.getStartIndex(), 0, "Split should have start offset 0 for whole partition");
  }

  private void verifyTopicManagerMethodCalls() {
    verify(topicManager).getStartPositionsForPartitionWithRetries(partition);
    verify(topicManager).getEndPositionsForPartitionWithRetries(partition);
    verify(topicManager).diffPosition(
        partition,
        topicManager.getEndPositionsForPartitionWithRetries(partition),
        topicManager.getStartPositionsForPartitionWithRetries(partition));
  }
}
