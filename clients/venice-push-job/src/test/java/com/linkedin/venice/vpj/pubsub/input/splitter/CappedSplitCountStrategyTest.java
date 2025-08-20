package com.linkedin.venice.vpj.pubsub.input.splitter;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

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
 * Unit tests for {@link CappedSplitCountStrategy}.
 */
public class CappedSplitCountStrategyTest {
  private static final PubSubTopicRepository TOPIC_REPOSITORY = new PubSubTopicRepository();

  private CappedSplitCountStrategy splitter;
  private TopicManager topicManager;
  private PubSubTopic topic;
  private PubSubTopicPartition partition;

  @BeforeMethod
  public void setUp() {
    topic = TOPIC_REPOSITORY.getTopic("test-topic");
    partition = new PubSubTopicPartitionImpl(topic, 0);
    topicManager = mock(TopicManager.class);
    splitter = new CappedSplitCountStrategy();
  }

  @Test
  public void testNormalSplitScenarios() {
    // Case 1: Even division - 10 records, 5 splits = 2 records each
    PubSubPosition startPos = ApacheKafkaOffsetPosition.of(100);
    PubSubPosition endPos = ApacheKafkaOffsetPosition.of(110);
    setupTopicManagerMocks(startPos, endPos, 10);
    setupAdvancePositionMocks(startPos, new long[] { 2, 2, 2, 2, 2 });

    SplitRequest request = createSplitRequest(5);
    List<PubSubPartitionSplit> splits = splitter.split(request);

    assertEquals(splits.size(), 5);
    verifyEvenSplits(splits, 2);
    verifyRangeIndexes(splits);
    verifyStartOffsets(splits);

    // Case 2: Uneven division - 11 records, 3 splits = [4, 4, 3]
    reset(topicManager);
    endPos = ApacheKafkaOffsetPosition.of(111);
    setupTopicManagerMocks(startPos, endPos, 11);
    setupAdvancePositionMocks(startPos, new long[] { 4, 4, 3 });

    request = createSplitRequest(3);
    splits = splitter.split(request);

    assertEquals(splits.size(), 3);
    assertEquals(splits.get(0).getNumberOfRecords(), 4L, "First split should have 4 records");
    verifySplitStartAndEndPosition(splits.get(0), startPos, ApacheKafkaOffsetPosition.of(104));
    assertEquals(splits.get(1).getNumberOfRecords(), 4L, "Second split should have 4 records");
    verifySplitStartAndEndPosition(splits.get(1), ApacheKafkaOffsetPosition.of(104), ApacheKafkaOffsetPosition.of(108));
    assertEquals(splits.get(2).getNumberOfRecords(), 3L, "Third split should have 3 records");
    verifySplitStartAndEndPosition(splits.get(2), ApacheKafkaOffsetPosition.of(108), endPos);
    verifyRangeIndexes(splits);
    verifyStartOffsets(splits);

    // Verify individual split indexes and start offsets for uneven division
    assertEquals(splits.get(0).getSplitIndex(), 0, "First split should have splitIndex 0");
    assertEquals(splits.get(0).getStartIndex(), 0L, "First split should have startOffset 0");
    assertEquals(splits.get(1).getSplitIndex(), 1, "Second split should have splitIndex 1");
    assertEquals(splits.get(1).getStartIndex(), 4L, "Second split should have startOffset 4");
    assertEquals(splits.get(2).getSplitIndex(), 2, "Third split should have splitIndex 2");
    assertEquals(splits.get(2).getStartIndex(), 8L, "Third split should have startOffset 8");

    // Case 3: More splits requested than records - 5 records, 10 splits = 5 splits of 1 each
    reset(topicManager);
    endPos = ApacheKafkaOffsetPosition.of(105);
    setupTopicManagerMocks(startPos, endPos, 5);
    setupAdvancePositionMocks(startPos, new long[] { 1, 1, 1, 1, 1 });

    request = createSplitRequest(10);
    splits = splitter.split(request);

    assertEquals(splits.size(), 5); // capped by number of records
    verifyEvenSplits(splits, 1);
    verifyRangeIndexes(splits);
    verifyStartOffsets(splits);
  }

  @Test
  public void testEdgeCaseSplitScenarios() {
    PubSubPosition startPos = ApacheKafkaOffsetPosition.of(0);
    PubSubPosition endPos;

    // Case 1: Single record, single split
    endPos = ApacheKafkaOffsetPosition.of(1);
    setupTopicManagerMocks(startPos, endPos, 1);
    setupAdvancePositionMocks(startPos, new long[] { 1 });

    SplitRequest request = createSplitRequest(1);
    List<PubSubPartitionSplit> splits = splitter.split(request);

    assertEquals(splits.size(), 1);
    assertEquals(splits.get(0).getNumberOfRecords(), 1L);
    assertEquals(splits.get(0).getSplitIndex(), 0);
    verifySplitStartAndEndPosition(splits.get(0), startPos, endPos);
    verifyRangeIndexes(splits);
    verifyStartOffsets(splits);

    // Case 2: Single record, multiple splits requested - should create only 1 split
    reset(topicManager);
    setupTopicManagerMocks(startPos, endPos, 1);
    setupAdvancePositionMocks(startPos, new long[] { 1 });

    request = createSplitRequest(5);
    splits = splitter.split(request);

    assertEquals(splits.size(), 1);
    assertEquals(splits.get(0).getNumberOfRecords(), 1L);
    verifyRangeIndexes(splits);
    verifyStartOffsets(splits);

    // Case 3: Large number of records with small maxSplits
    endPos = ApacheKafkaOffsetPosition.of(1000); // 1000 records
    reset(topicManager);
    setupTopicManagerMocks(startPos, endPos, 1000);
    setupAdvancePositionMocks(startPos, new long[] { 500, 500 });

    request = createSplitRequest(2);
    splits = splitter.split(request);

    assertEquals(splits.size(), 2);
    assertEquals(splits.get(0).getNumberOfRecords(), 500L);
    verifySplitStartAndEndPosition(splits.get(0), startPos, ApacheKafkaOffsetPosition.of(500));
    assertEquals(splits.get(1).getNumberOfRecords(), 500L);
    verifySplitStartAndEndPosition(splits.get(1), ApacheKafkaOffsetPosition.of(500), endPos);
    verifyRangeIndexes(splits);
    verifyStartOffsets(splits);
  }

  @Test
  public void testZeroAndNegativeRecordScenarios() {
    PubSubPosition startPos = ApacheKafkaOffsetPosition.of(100);
    PubSubPosition endPos;

    // Case 1: Zero records - should return empty list
    endPos = ApacheKafkaOffsetPosition.of(100);
    setupTopicManagerMocks(startPos, endPos, 0);

    SplitRequest request = createSplitRequest(5);
    List<PubSubPartitionSplit> splits = splitter.split(request);

    assertEquals(splits.size(), 0, "Zero records should return no splits");

    // Case 2: Negative record count should return no splits
    reset(topicManager);
    endPos = ApacheKafkaOffsetPosition.of(100);
    setupTopicManagerMocks(startPos, endPos, -5);

    request = createSplitRequest(3);
    splits = splitter.split(request);

    assertEquals(splits.size(), 0, "Negative records should return no splits");
  }

  @Test
  public void testDefaultSplitsValidation() {
    runMaxSplitsCase(0); // Case 1: Zero maxSplits, defaults to 5
    runMaxSplitsCase(-5); // Case 2: Negative maxSplits, defaults to 5
  }

  private void runMaxSplitsCase(int requestedMaxSplits) {
    // Common setup: 10 records from [0, 10), expect 5 even splits of size 2
    PubSubPosition startPos = ApacheKafkaOffsetPosition.of(0);
    PubSubPosition endPos = ApacheKafkaOffsetPosition.of(10);

    reset(topicManager);
    setupTopicManagerMocks(startPos, endPos, 10);
    setupAdvancePositionMocks(startPos, new long[] { 2, 2, 2, 2, 2 });

    SplitRequest request = createSplitRequest(requestedMaxSplits);
    List<PubSubPartitionSplit> splits = splitter.split(request);

    assertEquals(splits.size(), 5, "Expected 5 splits");
    assertEquals(splits.get(0).getNumberOfRecords(), 2L, "Each split should have 2 records");

    assertFiveEvenSplits(splits, 0L, 2L, endPos);
    verifyRangeIndexes(splits);
    verifyStartOffsets(splits);
  }

  private void assertFiveEvenSplits(
      List<PubSubPartitionSplit> splits,
      long startOffset,
      long step,
      PubSubPosition finalEndPos) {
    long s0 = startOffset;
    for (int i = 0; i < 5; i++) {
      PubSubPosition start = ApacheKafkaOffsetPosition.of(s0);
      PubSubPosition end = (i == 4) ? finalEndPos : ApacheKafkaOffsetPosition.of(s0 + step);
      verifySplitStartAndEndPosition(splits.get(i), start, end);
      s0 += step;
    }
  }

  @DataProvider(name = "remainderDistributionScenarios")
  public Object[][] remainderDistributionScenarios() {
    return new Object[][] {
        // {totalRecords, maxSplits, expectedSplitCounts}
        { 7, 3, new long[] { 3, 2, 2 } }, // 7 % 3 = 1, first split gets +1
        { 11, 4, new long[] { 3, 3, 3, 2 } }, // 11 % 4 = 3, first 3 splits get +1
        { 13, 5, new long[] { 3, 3, 3, 2, 2 } }, // 13 % 5 = 3, first 3 splits get +1
        { 17, 6, new long[] { 3, 3, 3, 3, 3, 2 } }, // 17 % 6 = 5, first 5 splits get +1
        { 20, 7, new long[] { 3, 3, 3, 3, 3, 3, 2 } } // 20 % 7 = 6, first 6 splits get +1
    };
  }

  @Test(dataProvider = "remainderDistributionScenarios")
  public void testRemainderDistribution(long totalRecords, int maxSplits, long[] expectedCounts) {
    PubSubPosition startPos = ApacheKafkaOffsetPosition.of(0);
    PubSubPosition endPos = ApacheKafkaOffsetPosition.of(totalRecords);

    setupTopicManagerMocks(startPos, endPos, totalRecords);
    setupAdvancePositionMocks(startPos, expectedCounts);

    SplitRequest request = createSplitRequest(maxSplits);
    List<PubSubPartitionSplit> splits = splitter.split(request);

    assertEquals(splits.size(), maxSplits);
    for (int i = 0; i < expectedCounts.length; i++) {
      assertEquals(
          splits.get(i).getNumberOfRecords(),
          expectedCounts[i],
          String.format(
              "Split %d should have %d records but has %d",
              i,
              expectedCounts[i],
              splits.get(i).getNumberOfRecords()));
    }

    // Verify total records add up correctly
    long totalSplitRecords = splits.stream().mapToLong(PubSubPartitionSplit::getNumberOfRecords).sum();
    assertEquals(totalSplitRecords, totalRecords);

    verifyRangeIndexes(splits);
    verifyStartOffsets(splits);
  }

  @Test
  public void testSplitPositionProgression() {
    // Case 1: Verify that splits have contiguous position ranges
    PubSubPosition startPos = ApacheKafkaOffsetPosition.of(100);
    PubSubPosition endPos = ApacheKafkaOffsetPosition.of(106);
    setupTopicManagerMocks(startPos, endPos, 6);

    PubSubPosition pos102 = ApacheKafkaOffsetPosition.of(102);
    PubSubPosition pos104 = ApacheKafkaOffsetPosition.of(104);
    PubSubPosition pos106 = ApacheKafkaOffsetPosition.of(106);

    when(topicManager.advancePosition(partition, startPos, 2)).thenReturn(pos102);
    when(topicManager.advancePosition(partition, pos102, 2)).thenReturn(pos104);
    when(topicManager.advancePosition(partition, pos104, 2)).thenReturn(pos106);

    SplitRequest request = createSplitRequest(3);
    List<PubSubPartitionSplit> splits = splitter.split(request);

    assertEquals(splits.size(), 3);

    // Verify position progression
    assertEquals(splits.get(0).getStartPubSubPosition(), startPos);
    assertEquals(splits.get(0).getEndPubSubPosition(), pos102);
    assertEquals(splits.get(1).getStartPubSubPosition(), pos102);
    assertEquals(splits.get(1).getEndPubSubPosition(), pos104);
    assertEquals(splits.get(2).getStartPubSubPosition(), pos104);
    assertEquals(splits.get(2).getEndPubSubPosition(), pos106);

    // Verify split indexes and start offsets for position progression
    assertEquals(splits.get(0).getSplitIndex(), 0, "First split should have splitIndex 0");
    assertEquals(splits.get(0).getStartIndex(), 0L, "First split should have startOffset 0");
    assertEquals(splits.get(1).getSplitIndex(), 1, "Second split should have splitIndex 1");
    assertEquals(splits.get(1).getStartIndex(), 2L, "Second split should have startOffset 2");
    assertEquals(splits.get(2).getSplitIndex(), 2, "Third split should have splitIndex 2");
    assertEquals(splits.get(2).getStartIndex(), 4L, "Third split should have startOffset 4");

    // Case 2: Verify all splits reference the same partition
    for (PubSubPartitionSplit split: splits) {
      assertEquals(split.getPubSubTopicPartition(), partition);
    }
    verifyRangeIndexes(splits);
    verifyStartOffsets(splits);
  }

  @Test
  public void testLargeScaleSplitting() {
    // Case 1: Large number of records with reasonable split count
    PubSubPosition startPos = ApacheKafkaOffsetPosition.of(0);
    PubSubPosition endPos = ApacheKafkaOffsetPosition.of(1000000);
    setupTopicManagerMocks(startPos, endPos, 1000000);

    // Create expected positions for 100 splits of 10000 records each
    PubSubPosition[] positions = new PubSubPosition[101]; // start + 100 end positions
    positions[0] = startPos;
    for (int i = 1; i <= 100; i++) {
      positions[i] = ApacheKafkaOffsetPosition.of(i * 10000L);
      when(topicManager.advancePosition(partition, positions[i - 1], 10000)).thenReturn(positions[i]);
    }

    SplitRequest request = createSplitRequest(100);
    List<PubSubPartitionSplit> splits = splitter.split(request);

    assertEquals(splits.size(), 100);
    verifyEvenSplits(splits, 10000);
    verifyRangeIndexes(splits);
    verifyStartOffsets(splits);

    // Verify first and last splits
    assertEquals(splits.get(0).getStartPubSubPosition(), startPos);
    assertEquals(splits.get(99).getEndPubSubPosition(), endPos);
  }

  private SplitRequest createSplitRequest(int maxSplits) {
    return new SplitRequest.Builder().pubSubTopicPartition(partition)
        .topicManager(topicManager)
        .splitType(PartitionSplitStrategy.CAPPED_SPLIT_COUNT)
        .maxSplits(maxSplits)
        .build();
  }

  private void setupTopicManagerMocks(PubSubPosition startPos, PubSubPosition endPos, long recordCount) {
    when(topicManager.getStartPositionsForPartitionWithRetries(partition)).thenReturn(startPos);
    when(topicManager.getEndPositionsForPartitionWithRetries(partition)).thenReturn(endPos);
    when(topicManager.diffPosition(partition, endPos, startPos)).thenReturn(recordCount);
    when(topicManager.getTopicRepository()).thenReturn(TOPIC_REPOSITORY);
  }

  private void setupAdvancePositionMocks(PubSubPosition startPos, long[] splitSizes) {
    PubSubPosition currentPos = startPos;
    long cumulativeOffset = ((ApacheKafkaOffsetPosition) startPos).getInternalOffset();

    for (long splitSize: splitSizes) {
      cumulativeOffset += splitSize;
      PubSubPosition nextPos = ApacheKafkaOffsetPosition.of(cumulativeOffset);
      when(topicManager.advancePosition(partition, currentPos, splitSize)).thenReturn(nextPos);
      currentPos = nextPos;
    }
  }

  private void verifyEvenSplits(List<PubSubPartitionSplit> splits, long expectedSize) {
    for (PubSubPartitionSplit split: splits) {
      assertEquals(split.getNumberOfRecords(), expectedSize);
      assertEquals(split.getPubSubTopicPartition(), partition);
    }
    verifyRangeIndexes(splits);
    verifyStartOffsets(splits);
  }

  private void verifyRangeIndexes(List<PubSubPartitionSplit> splits) {
    for (int i = 0; i < splits.size(); i++) {
      assertEquals(
          splits.get(i).getSplitIndex(),
          i,
          String.format("Split at index %d should have rangeIndex %d but has %d", i, i, splits.get(i).getSplitIndex()));
    }
  }

  private void verifyStartOffsets(List<PubSubPartitionSplit> splits) {
    long startOffset = 0;
    for (PubSubPartitionSplit split: splits) {
      assertEquals(split.getStartIndex(), startOffset);
      startOffset += split.getNumberOfRecords();
    }
  }

  private void verifySplitStartAndEndPosition(
      PubSubPartitionSplit split,
      PubSubPosition startPos,
      PubSubPosition endPos) {
    assertEquals(
        split.getStartPubSubPosition(),
        startPos,
        "Split start position does not match expected start position");
    assertEquals(split.getEndPubSubPosition(), endPos, "Split end position does not match expected end position");
  }
}
