package com.linkedin.venice.vpj.pubsub.input;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
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

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.vpj.pubsub.input.PubSubSplitIterator.OffsetMode;
import com.linkedin.venice.vpj.pubsub.input.PubSubSplitIterator.PubSubInputRecord;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class PubSubSplitIteratorTest {
  private static final PubSubTopicRepository TOPIC_REPOSITORY = new PubSubTopicRepository();
  private static final String TEST_TOPIC_NAME = "test_topic_v1";
  private static final int TEST_PARTITION = 0;

  private PubSubConsumerAdapter mockConsumer;
  private PubSubTopicPartition testTopicPartition;
  private PubSubTopic testTopic;
  private PubSubPartitionSplit testSplit;

  @BeforeMethod
  public void setUp() {
    mockConsumer = mock(PubSubConsumerAdapter.class);
    testTopic = TOPIC_REPOSITORY.getTopic(TEST_TOPIC_NAME);
    testTopicPartition = new PubSubTopicPartitionImpl(testTopic, TEST_PARTITION);
    testSplit = createTestSplit(0L, 10L, 10L, 0, 0L);
  }

  @Test
  public void testConstructorAndInitializationScenarios() throws IOException {
    // Case 1: Normal construction with NUMERIC mode
    PubSubSplitIterator iterator1 = new PubSubSplitIterator(mockConsumer, testSplit, false);
    assertEquals(iterator1.getTopicPartition(), testTopicPartition);
    assertEquals(iterator1.recordsRead(), 0L);
    assertNull(iterator1.getCurrentPosition());
    verify(mockConsumer).batchUnsubscribe(any());
    verify(mockConsumer).subscribe(eq(testTopicPartition), any(PubSubPosition.class), eq(true));
    iterator1.close();

    // Case 2: Construction with LOGICAL_INDEX mode
    PubSubConsumerAdapter mockConsumer2 = mock(PubSubConsumerAdapter.class);
    PubSubSplitIterator iterator2 = new PubSubSplitIterator(mockConsumer2, testSplit, true);
    assertEquals(iterator2.getTopicPartition(), testTopicPartition);
    iterator2.close();

    // Case 3: Construction with different split parameters
    PubSubPartitionSplit customSplit = createTestSplit(100L, 200L, 50L, 2, 150L);
    PubSubConsumerAdapter mockConsumer3 = mock(PubSubConsumerAdapter.class);
    PubSubSplitIterator iterator3 = new PubSubSplitIterator(mockConsumer3, customSplit, false);
    assertEquals(iterator3.getTopicPartition(), testTopicPartition);
    iterator3.close();
  }

  @Test
  public void testNextWithNormalDataRecords() throws IOException {
    // Case 1: Single data record consumption
    List<DefaultPubSubMessage> messages = createMockDataMessages(1, 0L);
    setupMockConsumerPolling(messages);
    when(mockConsumer.positionDifference(any(), any(), any())).thenReturn(10L, 9L);

    PubSubSplitIterator iterator1 = new PubSubSplitIterator(mockConsumer, testSplit, false);
    PubSubInputRecord record = iterator1.next();

    assertNotNull(record);
    assertEquals(record.getOffset(), 0L);
    assertEquals(iterator1.recordsRead(), 1L);
    iterator1.close();

    // Case 2: LOGICAL_INDEX mode offset calculation
    List<DefaultPubSubMessage> logicalMessages = createMockDataMessages(2, 50L);
    PubSubPartitionSplit logicalSplit = createTestSplit(50L, 60L, 10L, 1, 100L);
    PubSubConsumerAdapter mockConsumer3 = mock(PubSubConsumerAdapter.class);
    setupMockConsumerPolling(mockConsumer3, logicalMessages);
    when(mockConsumer3.positionDifference(any(), any(), any())).thenReturn(10L, 9L, 8L);

    PubSubSplitIterator iterator3 = new PubSubSplitIterator(mockConsumer3, logicalSplit, true);

    PubSubInputRecord record3 = iterator3.next();
    assertEquals(record3.getOffset(), 100L); // splitStartIndex + readSoFar (0)

    PubSubInputRecord record4 = iterator3.next();
    assertEquals(record4.getOffset(), 101L); // splitStartIndex + readSoFar (1)
    iterator3.close();

    // Case 3: Multiple data records consumption
    List<DefaultPubSubMessage> multiMessages = createMockDataMessages(3, 10L);
    PubSubConsumerAdapter mockConsumer2 = mock(PubSubConsumerAdapter.class);
    setupMockConsumerPolling(mockConsumer2, multiMessages);
    when(mockConsumer2.positionDifference(any(), any(), any())).thenReturn(10L, 9L, 8L, 7L);

    PubSubSplitIterator iterator2 = new PubSubSplitIterator(mockConsumer2, testSplit, false);
    PubSubInputRecord record1 = iterator2.next();
    assertEquals(record1.getOffset(), 10L);
    assertEquals(iterator2.recordsRead(), 1L);
    iterator2.close();

    // Case 4: Single record scenario
    List<DefaultPubSubMessage> singleMessage = createMockDataMessages(1, 1L);
    // Higher targetCount to allow reading
    PubSubPartitionSplit singleSplit = createTestSplit(0L, 2L, 5L, 0, 0L);
    PubSubConsumerAdapter mockConsumer4 = mock(PubSubConsumerAdapter.class);
    setupMockConsumerPolling(mockConsumer4, singleMessage);
    // Mock position difference: initially > 1 to allow hasNext() to return true, then <= 1 after reading
    when(mockConsumer4.positionDifference(any(), any(), any())).thenReturn(3L, 2L, 1L, 0L);

    PubSubSplitIterator singleIterator = new PubSubSplitIterator(mockConsumer4, singleSplit, false);
    assertTrue(singleIterator.hasNext());
    PubSubInputRecord singleRecord = singleIterator.next();
    assertNotNull(singleRecord);
    assertEquals(singleRecord.getOffset(), 1L);
    assertEquals(singleIterator.recordsRead(), 1L);
    singleIterator.close();
  }

  @Test
  public void testMixedControlAndDataMessages() throws IOException {
    List<DefaultPubSubMessage> mixed = new ArrayList<>();
    mixed.add(createMockControlMessage(0L));
    mixed.add(createMockDataMessage(1L));
    mixed.add(createMockControlMessage(2L));

    setupMockConsumerPolling(mixed);
    doReturn(10L).doReturn(9L).doReturn(8L).doReturn(7L).when(mockConsumer).positionDifference(any(), any(), any());

    PubSubSplitIterator iterator = new PubSubSplitIterator(mockConsumer, testSplit, false);

    PubSubInputRecord record = iterator.next();
    assertNotNull(record);
    assertEquals(record.getOffset(), 1L);
    assertEquals(iterator.recordsRead(), 2L); // 1 control + 1 data consumed
    iterator.close();
  }

  @Test
  public void testAllControlMessagesReturnNull() throws IOException {
    List<DefaultPubSubMessage> controlOnly = new ArrayList<>();
    controlOnly.add(createMockControlMessage(10L));
    controlOnly.add(createMockControlMessage(11L));

    PubSubConsumerAdapter mockConsumer2 = mock(PubSubConsumerAdapter.class);
    setupMockConsumerPolling(mockConsumer2, controlOnly);

    // MUST cross the (> 1) threshold so iterator can end cleanly.
    doReturn(2L).doReturn(1L).doReturn(0L).when(mockConsumer2).positionDifference(any(), any(), any());

    PubSubSplitIterator iterator = new PubSubSplitIterator(mockConsumer2, testSplit, false, 2, 3, 2);
    PubSubSplitIterator.PubSubInputRecord record = iterator.next();
    assertNull(record, "Should return null when only control messages are present");
    iterator.close();
  }

  @Test
  public void testControlMessageStatisticsTracking() throws IOException {
    List<DefaultPubSubMessage> statsMessages = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      statsMessages.add(createMockControlMessage(i));
    }
    statsMessages.add(createMockDataMessage(5L));

    PubSubConsumerAdapter mockConsumer3 = mock(PubSubConsumerAdapter.class);
    setupMockConsumerPolling(mockConsumer3, statsMessages);
    doReturn(10L).doReturn(9L)
        .doReturn(8L)
        .doReturn(7L)
        .doReturn(6L)
        .when(mockConsumer3)
        .positionDifference(any(), any(), any());

    PubSubSplitIterator iterator = new PubSubSplitIterator(mockConsumer3, testSplit, false);
    iterator.next();
    assertEquals(iterator.recordsRead(), 4L); // 3 control + 1 data
    iterator.close();
  }

  @Test
  public void testProgressCalculationScenarios() throws IOException {
    // Case 1: Progress with normal targetCount
    PubSubPartitionSplit progressSplit = createTestSplit(0L, 10L, 5L, 0, 0L);
    PubSubSplitIterator iterator1 = new PubSubSplitIterator(mockConsumer, progressSplit, false);

    assertEquals(iterator1.getProgress(), 0.0f);
    iterator1.setReadSoFar(2L);
    assertEquals(iterator1.getProgress(), 0.4f);
    iterator1.setReadSoFar(5L);
    assertEquals(iterator1.getProgress(), 1.0f);
    iterator1.close();

    // Case 2: Progress with zero targetCount
    PubSubPartitionSplit zeroSplit = createTestSplit(0L, 10L, 0L, 0, 0L);
    PubSubConsumerAdapter mockConsumer2 = mock(PubSubConsumerAdapter.class);
    PubSubSplitIterator iterator2 = new PubSubSplitIterator(mockConsumer2, zeroSplit, false);
    assertEquals(iterator2.getProgress(), 1.0f);
    iterator2.close();

    // Case 3: Progress capping at 100%
    PubSubPartitionSplit cappingSplit = createTestSplit(0L, 10L, 3L, 0, 0L);
    PubSubConsumerAdapter mockConsumer3 = mock(PubSubConsumerAdapter.class);
    PubSubSplitIterator iterator3 = new PubSubSplitIterator(mockConsumer3, cappingSplit, false);
    iterator3.setReadSoFar(5L); // More than target
    assertEquals(iterator3.getProgress(), 1.0f);
    iterator3.close();
  }

  @Test
  public void testCloseMethodScenarios() {
    // Case 1: Single close call
    PubSubSplitIterator iterator1 = new PubSubSplitIterator(mockConsumer, testSplit, false);
    iterator1.close();
    verify(mockConsumer).close();
    assertTrue(iterator1.isClosed());

    // Case 2: Multiple close calls (idempotent)
    PubSubConsumerAdapter mockConsumer2 = mock(PubSubConsumerAdapter.class);
    PubSubSplitIterator iterator2 = new PubSubSplitIterator(mockConsumer2, testSplit, false);
    iterator2.close();
    iterator2.close();
    verify(mockConsumer2, times(1)).close();
  }

  @Test
  public void testPubSubInputRecordInnerClass() {
    // Case 1: Basic construction and getters
    DefaultPubSubMessage mockMessage = createMockDataMessage(42L);
    PubSubInputRecord record1 = new PubSubInputRecord(mockMessage, 100L);
    assertEquals(record1.getPubSubMessage(), mockMessage);
    assertEquals(record1.getOffset(), 100L);

    // Case 2: Different offset values
    PubSubInputRecord record2 = new PubSubInputRecord(mockMessage, 0L);
    assertEquals(record2.getOffset(), 0L);

    PubSubInputRecord record3 = new PubSubInputRecord(mockMessage, Long.MAX_VALUE);
    assertEquals(record3.getOffset(), Long.MAX_VALUE);

    // Case 3: Same message, different offsets
    PubSubInputRecord record4 = new PubSubInputRecord(mockMessage, 999L);
    PubSubInputRecord record5 = new PubSubInputRecord(mockMessage, 888L);
    assertEquals(record4.getPubSubMessage(), record5.getPubSubMessage());
    assertEquals(record4.getOffset(), 999L);
    assertEquals(record5.getOffset(), 888L);
  }

  @Test(dataProvider = "offsetModeProvider")
  public void testOffsetModeVariations(boolean useLogicalIndex, OffsetMode expectedMode) throws IOException {
    List<DefaultPubSubMessage> messages = createMockDataMessages(1, 75L);
    PubSubPartitionSplit modeSplit = createTestSplit(0L, 10L, 10L, 0, 300L);
    setupMockConsumerPolling(messages);
    when(mockConsumer.positionDifference(any(), any(), any())).thenReturn(10L, 9L);

    PubSubSplitIterator iterator = new PubSubSplitIterator(mockConsumer, modeSplit, useLogicalIndex);
    PubSubInputRecord record = iterator.next();

    if (expectedMode == OffsetMode.LOGICAL_INDEX) {
      assertEquals(record.getOffset(), 300L);
    } else {
      assertEquals(record.getOffset(), 75L);
    }
    iterator.close();
  }

  @Test
  public void testEmptyPollAfterRetries() {
    when(mockConsumer.poll(anyLong())).thenReturn(Collections.emptyMap());
    when(mockConsumer.positionDifference(any(), any(), any())).thenReturn(10L);

    PubSubSplitIterator iterator = new PubSubSplitIterator(mockConsumer, testSplit, false, 2, 3, 2);
    VeniceException exception = expectThrows(VeniceException.class, iterator::next);
    assertTrue(exception.getMessage().contains("Empty poll after"));
    verify(mockConsumer, times(3)).poll(anyLong());
    iterator.close();
  }

  @Test
  public void testInterruptedDuringPolling() {
    PubSubConsumerAdapter mockConsumer2 = mock(PubSubConsumerAdapter.class);
    when(mockConsumer2.positionDifference(any(), any(), any())).thenReturn(10L);
    doAnswer(invocation -> {
      Thread.currentThread().interrupt();
      return Collections.emptyMap();
    }).when(mockConsumer2).poll(anyLong());

    PubSubSplitIterator iterator = new PubSubSplitIterator(mockConsumer2, testSplit, false);
    VeniceException exception = expectThrows(VeniceException.class, iterator::next);
    assertTrue(exception.getMessage().contains("Interrupted while fetching"));
    iterator.close();
  }

  @Test
  public void testNullMessageFromIterator() {
    Map<PubSubTopicPartition, List<DefaultPubSubMessage>> nullMessageMap = new HashMap<>();
    nullMessageMap.put(testTopicPartition, Collections.singletonList(null));

    PubSubConsumerAdapter mockConsumer3 = mock(PubSubConsumerAdapter.class);
    when(mockConsumer3.positionDifference(any(), any(), any())).thenReturn(10L);
    when(mockConsumer3.poll(anyLong())).thenReturn(nullMessageMap);

    PubSubSplitIterator iterator = new PubSubSplitIterator(mockConsumer3, testSplit, false);
    IOException exception = expectThrows(IOException.class, iterator::next);
    assertTrue(exception.getMessage().contains("Unable to read additional data"));
    iterator.close();
  }

  @Test
  public void testEdgeCaseScenarios() throws IOException {
    // Case 1: Empty target count
    PubSubPartitionSplit emptySplit = createTestSplit(0L, 10L, 0L, 0, 0L);
    PubSubSplitIterator emptyIterator = new PubSubSplitIterator(mockConsumer, emptySplit, false);
    assertFalse(emptyIterator.hasNext());
    assertNull(emptyIterator.next());
    assertEquals(emptyIterator.getProgress(), 1.0f);
    emptyIterator.close();

    // Case 2: Single record scenario
    List<DefaultPubSubMessage> singleMessage = createMockDataMessages(1, 1L);
    PubSubPartitionSplit singleSplit = createTestSplit(0L, 2L, 5L, 0, 0L); // Higher targetCount to allow reading
    PubSubConsumerAdapter mockConsumer2 = mock(PubSubConsumerAdapter.class);
    setupMockConsumerPolling(mockConsumer2, singleMessage);
    // Mock position difference: initially > 1 to allow hasNext() to return true, then <= 1 after reading
    when(mockConsumer2.positionDifference(any(), any(), any())).thenReturn(3L, 2L, 1L, 0L);

    PubSubSplitIterator singleIterator = new PubSubSplitIterator(mockConsumer2, singleSplit, false);
    assertTrue(singleIterator.hasNext());
    PubSubInputRecord record = singleIterator.next();
    assertNotNull(record);
    assertEquals(record.getOffset(), 1L);
    assertEquals(singleIterator.recordsRead(), 1L);
    singleIterator.close();

    // Case 3: hasNext boundary conditions
    when(mockConsumer.positionDifference(any(), any(), any())).thenReturn(1L); // At boundary
    PubSubSplitIterator boundaryIterator = new PubSubSplitIterator(mockConsumer, testSplit, false);
    boundaryIterator.setCurrentPosition(ApacheKafkaOffsetPosition.of(9L));
    assertFalse(boundaryIterator.hasNext());
    boundaryIterator.close();
  }

  @DataProvider(name = "offsetModeProvider")
  public Object[][] offsetModeProvider() {
    return new Object[][] { { false, OffsetMode.NUMERIC }, { true, OffsetMode.LOGICAL_INDEX } };
  }

  // Helper Methods
  private PubSubPartitionSplit createTestSplit(
      long startOffset,
      long endOffset,
      long numberOfRecords,
      int splitIndex,
      long startIndex) {
    PubSubPosition startPos = ApacheKafkaOffsetPosition.of(startOffset);
    PubSubPosition endPos = ApacheKafkaOffsetPosition.of(endOffset);
    return new PubSubPartitionSplit(
        TOPIC_REPOSITORY,
        testTopicPartition,
        startPos,
        endPos,
        numberOfRecords,
        splitIndex,
        startIndex);
  }

  private List<DefaultPubSubMessage> createMockDataMessages(int count, long startOffset) {
    List<DefaultPubSubMessage> messages = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      messages.add(createMockDataMessage(startOffset + i));
    }
    return messages;
  }

  private DefaultPubSubMessage createMockDataMessage(long offset) {
    DefaultPubSubMessage message = mock(DefaultPubSubMessage.class);
    KafkaKey key = mock(KafkaKey.class);
    when(key.isControlMessage()).thenReturn(false);
    when(message.getKey()).thenReturn(key);
    when(message.getPosition()).thenReturn(ApacheKafkaOffsetPosition.of(offset));
    return message;
  }

  private DefaultPubSubMessage createMockControlMessage(long offset) {
    DefaultPubSubMessage message = mock(DefaultPubSubMessage.class);
    KafkaKey key = mock(KafkaKey.class);
    when(key.isControlMessage()).thenReturn(true);
    when(message.getKey()).thenReturn(key);
    when(message.getPosition()).thenReturn(ApacheKafkaOffsetPosition.of(offset));
    return message;
  }

  private void setupMockConsumerPolling(List<DefaultPubSubMessage> messages) {
    setupMockConsumerPolling(mockConsumer, messages);
  }

  private void setupMockConsumerPolling(PubSubConsumerAdapter consumer, List<DefaultPubSubMessage> messages) {
    Map<PubSubTopicPartition, List<DefaultPubSubMessage>> pollResult = new HashMap<>();
    pollResult.put(testTopicPartition, messages);
    when(consumer.poll(anyLong())).thenReturn(pollResult, Collections.emptyMap());
  }
}
