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
 * Unit tests for FixedRecordCountSplitStrategy.
 */
public class FixedRecordCountSplitStrategyTest {
  private static final PubSubTopicRepository TOPIC_REPOSITORY = new PubSubTopicRepository();

  private FixedRecordCountSplitStrategy splitter;
  private TopicManager topicManager;
  private PubSubTopic topic;
  private PubSubTopicPartition partition;

  @BeforeMethod
  public void setUp() {
    topic = TOPIC_REPOSITORY.getTopic("test-topic");
    partition = new PubSubTopicPartitionImpl(topic, 0);
    topicManager = mock(TopicManager.class);
    splitter = new FixedRecordCountSplitStrategy();
  }

  @Test
  public void testNormalSplitScenarios() {
    // Case 1: Even division - 10 records, 2 per split = 5 splits
    PubSubPosition startPos = ApacheKafkaOffsetPosition.of(100);
    PubSubPosition endPos = ApacheKafkaOffsetPosition.of(110);
    setupTopicManagerMocks(startPos, endPos, 10);
    setupAdvancePositionMocks(startPos, new long[] { 2, 2, 2, 2, 2 });

    SplitRequest request = createSplitRequest(2);
    List<PubSubPartitionSplit> splits = splitter.split(request);

    assertEquals(splits.size(), 5);
    verifyEvenSplits(splits, 2);
    verifyRangeIndexes(splits);
    verifyStartOffsets(splits);

    // Case 2: Uneven division - 11 records, 3 per split = [3, 3, 3, 2]
    reset(topicManager);
    endPos = ApacheKafkaOffsetPosition.of(111);
    setupTopicManagerMocks(startPos, endPos, 11);
    setupAdvancePositionMocks(startPos, new long[] { 3, 3, 3, 2 });

    request = createSplitRequest(3);
    splits = splitter.split(request);

    assertEquals(splits.size(), 4);
    assertEquals(splits.get(0).getNumberOfRecords(), 3L, "First split should have 3 records");
    verifySplitStartAndEndPosition(splits.get(0), startPos, ApacheKafkaOffsetPosition.of(103));
    assertEquals(splits.get(1).getNumberOfRecords(), 3L, "Second split should have 3 records");
    verifySplitStartAndEndPosition(splits.get(1), ApacheKafkaOffsetPosition.of(103), ApacheKafkaOffsetPosition.of(106));
    assertEquals(splits.get(2).getNumberOfRecords(), 3L, "Third split should have 3 records");
    verifySplitStartAndEndPosition(splits.get(2), ApacheKafkaOffsetPosition.of(106), ApacheKafkaOffsetPosition.of(109));
    assertEquals(splits.get(3).getNumberOfRecords(), 2L, "Fourth split should have 2 records");
    verifySplitStartAndEndPosition(splits.get(3), ApacheKafkaOffsetPosition.of(109), endPos);
    verifyRangeIndexes(splits);
    verifyStartOffsets(splits);

    // Verify individual split indexes and start offsets for uneven division
    assertEquals(splits.get(0).getSplitIndex(), 0, "First split should have splitIndex 0");
    assertEquals(splits.get(0).getStartIndex(), 0L, "First split should have startOffset 0");
    assertEquals(splits.get(1).getSplitIndex(), 1, "Second split should have splitIndex 1");
    assertEquals(splits.get(1).getStartIndex(), 3L, "Second split should have startOffset 3");
    assertEquals(splits.get(2).getSplitIndex(), 2, "Third split should have splitIndex 2");
    assertEquals(splits.get(2).getStartIndex(), 6L, "Third split should have startOffset 6");
    assertEquals(splits.get(3).getSplitIndex(), 3, "Fourth split should have splitIndex 3");
    assertEquals(splits.get(3).getStartIndex(), 9L, "Fourth split should have startOffset 9");

    // Case 3: Single record per split - 5 records, 1 per split = 5 splits of 1 each
    reset(topicManager);
    endPos = ApacheKafkaOffsetPosition.of(105);
    setupTopicManagerMocks(startPos, endPos, 5);
    setupAdvancePositionMocks(startPos, new long[] { 1, 1, 1, 1, 1 });

    request = createSplitRequest(1);
    splits = splitter.split(request);

    assertEquals(splits.size(), 5);
    verifyEvenSplits(splits, 1);
    verifyRangeIndexes(splits);
    verifyStartOffsets(splits);
  }

  @Test
  public void testEdgeCaseSplitScenarios() {
    PubSubPosition startPos = ApacheKafkaOffsetPosition.of(100);
    PubSubPosition endPos;

    // Case 1: Empty partition - 0 records
    endPos = ApacheKafkaOffsetPosition.of(100);
    setupTopicManagerMocks(startPos, endPos, 0);

    SplitRequest request = createSplitRequest(5);
    List<PubSubPartitionSplit> splits = splitter.split(request);

    assertEquals(splits.size(), 0, "Empty partition should return no splits");

    // Case 2: Single record total with recordsPerSplit > 1
    reset(topicManager);
    endPos = ApacheKafkaOffsetPosition.of(101);
    setupTopicManagerMocks(startPos, endPos, 1);
    setupAdvancePositionMocks(startPos, new long[] { 1 });

    request = createSplitRequest(5);
    splits = splitter.split(request);

    assertEquals(splits.size(), 1);
    assertEquals(splits.get(0).getNumberOfRecords(), 1L);
    verifySplitStartAndEndPosition(splits.get(0), startPos, endPos);
    verifyRangeIndexes(splits);
    verifyStartOffsets(splits);

    // Case 3: recordsPerSplit larger than total records - 3 records, 10 per split = 1 split of 3
    reset(topicManager);
    endPos = ApacheKafkaOffsetPosition.of(103);
    setupTopicManagerMocks(startPos, endPos, 3);
    setupAdvancePositionMocks(startPos, new long[] { 3 });

    request = createSplitRequest(10);
    splits = splitter.split(request);

    assertEquals(splits.size(), 1);
    assertEquals(splits.get(0).getNumberOfRecords(), 3L);
    verifySplitStartAndEndPosition(splits.get(0), startPos, endPos);
    verifyRangeIndexes(splits);
    verifyStartOffsets(splits);
  }

  @Test
  public void testInvalidRecordsPerSplitFallbackScenarios() {
    PubSubPosition startPos = ApacheKafkaOffsetPosition.of(1000);
    PubSubPosition endPos = ApacheKafkaOffsetPosition.of(20001000); // 20M records: 1000 + 20M = 20,001,000
    setupTopicManagerMocks(startPos, endPos, 20000000);

    // With 20M records and default recordsPerSplit (5M), expect 4 splits: [5M, 5M, 5M, 5M]
    setupAdvancePositionMocks(startPos, new long[] { 5000000, 5000000, 5000000, 5000000 });

    // Case 1: recordsPerSplit = 0 falls back to default
    SplitRequest request = createSplitRequest(0);
    List<PubSubPartitionSplit> splits = splitter.split(request);

    assertEquals(splits.size(), 4, "Should create 4 splits with 20M records and default recordsPerSplit of 5M");
    for (int i = 0; i < 4; i++) {
      assertEquals(splits.get(i).getNumberOfRecords(), 5000000L, String.format("Split %d should have 5M records", i));
    }
    verifyRangeIndexes(splits);
    verifyStartOffsets(splits);

    // Verify first and last split positions
    assertEquals(splits.get(0).getStartPubSubPosition(), startPos);
    assertEquals(splits.get(3).getEndPubSubPosition(), endPos);
  }

  @Test
  public void testNegativeRecordsPerSplitFallback() {
    PubSubPosition startPos = ApacheKafkaOffsetPosition.of(2000);
    PubSubPosition endPos = ApacheKafkaOffsetPosition.of(25002000); // 25M records: 2000 + 25M = 25,002,000
    setupTopicManagerMocks(startPos, endPos, 25000000);

    // With 25M records and default recordsPerSplit (5M), expect 5 splits: [5M, 5M, 5M, 5M, 5M]
    setupAdvancePositionMocks(startPos, new long[] { 5000000, 5000000, 5000000, 5000000, 5000000 });

    // Case 1: recordsPerSplit < 0 falls back to default
    SplitRequest request = createSplitRequest(-5);
    List<PubSubPartitionSplit> splits = splitter.split(request);

    assertEquals(splits.size(), 5, "Should create 5 splits with 25M records and default recordsPerSplit of 5M");
    for (int i = 0; i < 5; i++) {
      assertEquals(splits.get(i).getNumberOfRecords(), 5000000L, String.format("Split %d should have 5M records", i));
    }
    verifyRangeIndexes(splits);
    verifyStartOffsets(splits);

    // Verify first and last split positions
    assertEquals(splits.get(0).getStartPubSubPosition(), startPos);
    assertEquals(splits.get(4).getEndPubSubPosition(), endPos);
  }

  @DataProvider(name = "remainderDistributionData")
  public Object[][] remainderDistributionData() {
    return new Object[][] {
        // {totalRecords, recordsPerSplit, expectedSplitCount, expectedLastSplitSize}
        { 10, 3, 4, 1 }, // 10 records, 3 per split: [3, 3, 3, 1]
        { 15, 4, 4, 3 }, // 15 records, 4 per split: [4, 4, 4, 3]
        { 100, 7, 15, 2 }, // 100 records, 7 per split: 14 splits of 7 + 1 split of 2
        { 50, 8, 7, 2 }, // 50 records, 8 per split: 6 splits of 8 + 1 split of 2
        { 13, 5, 3, 3 } // 13 records, 5 per split: [5, 5, 3]
    };
  }

  @Test(dataProvider = "remainderDistributionData")
  public void testRemainderDistribution(
      int totalRecords,
      int recordsPerSplit,
      int expectedSplitCount,
      int expectedLastSplitSize) {
    PubSubPosition startPos = ApacheKafkaOffsetPosition.of(1000);
    PubSubPosition endPos = ApacheKafkaOffsetPosition.of(1000 + totalRecords);
    setupTopicManagerMocks(startPos, endPos, totalRecords);

    // Setup advance position mocks for the expected split pattern
    long[] splitSizes = new long[expectedSplitCount];
    for (int i = 0; i < expectedSplitCount - 1; i++) {
      splitSizes[i] = recordsPerSplit;
    }
    splitSizes[expectedSplitCount - 1] = expectedLastSplitSize;
    setupAdvancePositionMocks(startPos, splitSizes);

    SplitRequest request = createSplitRequest(recordsPerSplit);
    List<PubSubPartitionSplit> splits = splitter.split(request);

    assertEquals(splits.size(), expectedSplitCount);

    // Verify all splits except last have recordsPerSplit records
    for (int i = 0; i < splits.size() - 1; i++) {
      assertEquals(
          splits.get(i).getNumberOfRecords(),
          (long) recordsPerSplit,
          String.format("Split %d should have %d records", i, recordsPerSplit));
    }

    // Verify last split has expected remainder
    assertEquals(
        splits.get(splits.size() - 1).getNumberOfRecords(),
        (long) expectedLastSplitSize,
        "Last split should have expected remainder records");

    verifyRangeIndexes(splits);
    verifyStartOffsets(splits);
  }

  @Test
  public void testSplitPositionProgression() {
    // Case 1: Verify contiguous position ranges
    PubSubPosition startPos = ApacheKafkaOffsetPosition.of(200);
    PubSubPosition pos202 = ApacheKafkaOffsetPosition.of(202);
    PubSubPosition pos204 = ApacheKafkaOffsetPosition.of(204);
    PubSubPosition pos206 = ApacheKafkaOffsetPosition.of(206);
    setupTopicManagerMocks(startPos, pos206, 6);
    setupAdvancePositionMocks(startPos, new long[] { 2, 2, 2 });

    SplitRequest request = createSplitRequest(2);
    List<PubSubPartitionSplit> splits = splitter.split(request);

    assertEquals(splits.size(), 3);

    // Verify position progression
    assertEquals(splits.get(0).getStartPubSubPosition(), startPos);
    assertEquals(splits.get(0).getEndPubSubPosition(), pos202);
    assertEquals(splits.get(1).getStartPubSubPosition(), pos202);
    assertEquals(splits.get(1).getEndPubSubPosition(), pos204);
    assertEquals(splits.get(2).getStartPubSubPosition(), pos204);
    assertEquals(splits.get(2).getEndPubSubPosition(), pos206);

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
    // Case 1: Large number of records with small recordsPerSplit
    PubSubPosition startPos = ApacheKafkaOffsetPosition.of(0);
    PubSubPosition endPos = ApacheKafkaOffsetPosition.of(100000);
    setupTopicManagerMocks(startPos, endPos, 100000);

    // Create expected positions for 10000 splits of 10 records each
    PubSubPosition[] positions = new PubSubPosition[10001]; // start + 10000 end positions
    positions[0] = startPos;
    for (int i = 1; i <= 10000; i++) {
      positions[i] = ApacheKafkaOffsetPosition.of(i * 10L);
      when(topicManager.advancePosition(partition, positions[i - 1], 10)).thenReturn(positions[i]);
    }

    SplitRequest request = createSplitRequest(10);
    List<PubSubPartitionSplit> splits = splitter.split(request);

    assertEquals(splits.size(), 10000);
    verifyEvenSplits(splits, 10);

    // Verify first and last splits
    assertEquals(splits.get(0).getStartPubSubPosition(), startPos);
    assertEquals(splits.get(9999).getEndPubSubPosition(), endPos);
    verifyRangeIndexes(splits);
    verifyStartOffsets(splits);
  }

  private SplitRequest createSplitRequest(long recordsPerSplit) {
    return new SplitRequest.Builder().pubSubTopicPartition(partition)
        .topicManager(topicManager)
        .splitType(PartitionSplitStrategy.FIXED_RECORD_COUNT)
        .recordsPerSplit(recordsPerSplit)
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
          String.format("Split at index %d should have splitIndex %d but has %d", i, i, splits.get(i).getSplitIndex()));
    }
  }

  private void verifyStartOffsets(List<PubSubPartitionSplit> splits) {
    long expectedStartOffset = 0L;
    for (int i = 0; i < splits.size(); i++) {
      assertEquals(
          splits.get(i).getStartIndex(),
          expectedStartOffset,
          String.format(
              "Split at index %d should have startOffset %d but has %d",
              i,
              expectedStartOffset,
              splits.get(i).getStartIndex()));
      expectedStartOffset += splits.get(i).getNumberOfRecords();
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
