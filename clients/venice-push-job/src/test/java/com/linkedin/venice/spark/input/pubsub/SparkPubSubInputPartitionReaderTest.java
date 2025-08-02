package com.linkedin.venice.spark.input.pubsub;

import static org.mockito.Mockito.when;

import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.vpj.pubsub.input.PubSubPartitionSplit;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.spark.sql.catalyst.InternalRow;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link SparkPubSubInputPartitionReader}.
 * This test class validates the functionality of the partition reader including:
 * - Reading messages from PubSub topic partition
 * - Handling of starting and ending offsets
 * - Conversion of PubSub messages to Spark InternalRow format
 * - Filtering of control messages
 * - Progress tracking and reporting
 * - Error conditions and edge cases
 */
public class SparkPubSubInputPartitionReaderTest {
  private static final PubSubTopicRepository TOPIC_REPOSITORY = new PubSubTopicRepository();
  // short timeouts and retry counts to speed up tests
  static final int CONSUMER_POLL_EMPTY_RESULT_RETRY_TIMES = 3;
  static final long EMPTY_POLL_SLEEP_TIME_MS = TimeUnit.SECONDS.toMillis(1);
  static final long CONSUMER_POLL_TIMEOUT = TimeUnit.SECONDS.toMillis(1); // 1 second
  @Mock
  private SparkPubSubInputPartition mockInputPartition;
  @Mock
  private PubSubConsumerAdapter mockConsumer;
  @Mock
  private VenicePubSubMessageToRow mockMessageToRowConverter;

  private SparkPubSubInputPartitionReader reader;
  private PubSubTopic pubSubTopic;
  private PubSubTopicPartition topicPartition;
  private PubSubPartitionSplit pubSubPartitionSplit;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);

    // basic setup
    int partitionNumber = 5;
    String topicName = "basic-mock-topic_v1";
    String region = "basic-mock-region1";
    PubSubPosition startPosition = ApacheKafkaOffsetPosition.of(1L);
    PubSubPosition endPosition = ApacheKafkaOffsetPosition.of(2L);

    pubSubTopic = TOPIC_REPOSITORY.getTopic(topicName);
    topicPartition = new PubSubTopicPartitionImpl(pubSubTopic, partitionNumber);
    pubSubPartitionSplit =
        new PubSubPartitionSplit(TOPIC_REPOSITORY, topicPartition, startPosition, endPosition, 1L, 0, 1L);
    when(mockInputPartition.getPubSubPartitionSplit()).thenReturn(pubSubPartitionSplit);
  }

  @AfterMethod
  public void tearDown() {
    if (reader != null) {
      reader.close();
    }
  }

  /**
  * This is a test for a topic that despite having start and end offsets, has no messages.
  */
  @Test
  public void testEmptyTopicInitialization() {
    String region = "ei-test-region2";
    long start = System.currentTimeMillis();
    reader = new SparkPubSubInputPartitionReader(
        mockInputPartition,
        mockConsumer,
        topicPartition,
        region,
        false,
        CONSUMER_POLL_TIMEOUT,
        CONSUMER_POLL_EMPTY_RESULT_RETRY_TIMES,
        EMPTY_POLL_SLEEP_TIME_MS,
        new VenicePubSubMessageToRow());
    Assert.assertFalse(reader.next()); // Attempt to read messages, which should fail due to empty topic
    long elapsed = System.currentTimeMillis() - start;
    Assert.assertTrue(
        elapsed >= CONSUMER_POLL_EMPTY_RESULT_RETRY_TIMES * CONSUMER_POLL_TIMEOUT,
        "Constructor should take at least 3 seconds due to polling retries.");
    Assert.assertTrue(
        elapsed < (CONSUMER_POLL_EMPTY_RESULT_RETRY_TIMES + 1) * CONSUMER_POLL_TIMEOUT,
        "Constructor should not exceed 4 seconds, it doesn't do much after failure.");

    reader.close();
  }

  @Test
  public void testShortTopicNoControlMessage() {
    // Mock tuning
    String region = "ei-test-region2";
    int partitionNumber = 5;
    long startOffset = 1L;

    pubSubPartitionSplit = new PubSubPartitionSplit(
        TOPIC_REPOSITORY,
        topicPartition,
        ApacheKafkaOffsetPosition.of(startOffset),
        ApacheKafkaOffsetPosition.of(startOffset + 2),
        2L,
        0,
        1L);
    when(mockInputPartition.getPubSubPartitionSplit()).thenReturn(pubSubPartitionSplit);

    // prepare 2 mock messages

    DefaultPubSubMessage mockMessage1 = createMockMessage(startOffset, false);
    DefaultPubSubMessage mockMessage2 = createMockMessage(startOffset + 1L, false);

    // Use helper method to create poll result maps
    Map<PubSubTopicPartition, List<DefaultPubSubMessage>> pollResult1 =
        createPollResultMap(topicPartition, mockMessage1);
    Map<PubSubTopicPartition, List<DefaultPubSubMessage>> pollResult2 =
        createPollResultMap(topicPartition, mockMessage2);

    when(mockConsumer.poll(CONSUMER_POLL_TIMEOUT)).thenReturn(pollResult1) // first poll returns a map with first
        // message
        .thenReturn(pollResult2) // second poll returns a map with second message
        .thenReturn(new HashMap<>()); // third poll returns an empty map, indicating no more messages

    // mock the return value of the message converter
    InternalRow mockRow = Mockito.mock(InternalRow.class);
    when(
        mockMessageToRowConverter.convert(
            Mockito.eq(mockMessage1),
            Mockito.eq(region),
            Mockito.eq(partitionNumber),
            Mockito.eq(startOffset))).thenReturn(mockRow);
    when(
        mockMessageToRowConverter.convert(
            Mockito.eq(mockMessage2),
            Mockito.eq(region),
            Mockito.eq(partitionNumber),
            Mockito.eq(startOffset + 1L))).thenReturn(mockRow);

    reader = new SparkPubSubInputPartitionReader(
        mockInputPartition,
        mockConsumer,
        topicPartition,
        region,
        false,
        CONSUMER_POLL_TIMEOUT,
        CONSUMER_POLL_EMPTY_RESULT_RETRY_TIMES,
        EMPTY_POLL_SLEEP_TIME_MS,
        mockMessageToRowConverter);

    Assert.assertEquals(reader.getProgressPercent(), 0f, "we should report 0% progress.");
    Assert.assertTrue(reader.next(), "Reader should have a message available.");
    Assert.assertNotNull(reader.get(), "Expected one result.");
    Assert.assertEquals(reader.getProgressPercent(), 50f, "we should report 50% progress.");
    Assert.assertEquals(reader.get(), mockRow, "Reader should return the mockRow.");
    Assert.assertTrue(reader.next(), "Reader should have a second message available.");
    Assert.assertEquals(reader.getProgressPercent(), 100f, "We should now report 100% progress.");
    Assert.assertFalse(reader.next(), "Reader should not have any more messages available.");

    // at this point, we keep returning the last good row, and there wont be any "next" message available.
    Assert.assertFalse(reader.next(), "Reader should not have any more messages available.");
    Assert.assertEquals(reader.get(), mockRow, "Reader should still return the mockRow.");

    reader.close();
  }

  @Test
  public void testFilterControlMessages() {
    // Mock tuning
    String region = "prod-test-region1";
    long startOffset = 1L;
    int partitionNumber = 5;

    // first mock message is a control message, second is a regular message
    DefaultPubSubMessage mockMessage1 = createMockMessage(startOffset, true);
    DefaultPubSubMessage mockMessage2 = createMockMessage(startOffset + 1L, false);

    // Use helper method to create poll result maps
    Map<PubSubTopicPartition, List<DefaultPubSubMessage>> pollResult1 =
        createPollResultMap(topicPartition, mockMessage1);
    Map<PubSubTopicPartition, List<DefaultPubSubMessage>> pollResult2 =
        createPollResultMap(topicPartition, mockMessage2);

    when(mockConsumer.poll(CONSUMER_POLL_TIMEOUT)).thenReturn(pollResult1) // First poll returns a map with one control
        // message
        .thenReturn(pollResult2) // Second poll returns a map with one regular message
        .thenReturn(new HashMap<>()); // Third poll returns an empty map, indicating no more messages

    InternalRow mockRow = Mockito.mock(InternalRow.class);
    // mock the return value of the message converter
    when(
        mockMessageToRowConverter.convert(
            Mockito.eq(mockMessage1),
            Mockito.eq(region),
            Mockito.eq(partitionNumber),
            Mockito.eq(startOffset))).thenReturn(mockRow);
    when(
        mockMessageToRowConverter.convert(
            Mockito.eq(mockMessage2),
            Mockito.eq(region),
            Mockito.eq(partitionNumber),
            Mockito.eq(startOffset + 1L))).thenReturn(mockRow);

    reader = new SparkPubSubInputPartitionReader(
        mockInputPartition,
        mockConsumer,
        topicPartition,
        region,
        false,
        CONSUMER_POLL_TIMEOUT,
        CONSUMER_POLL_EMPTY_RESULT_RETRY_TIMES,
        EMPTY_POLL_SLEEP_TIME_MS,
        mockMessageToRowConverter);

    // there should be a message available,
    Assert.assertEquals(reader.getProgressPercent(), 0f, "we should report 0% progress.");
    Assert.assertTrue(reader.next(), "Reader should have a message available.");
    Assert.assertEquals(reader.getProgressPercent(), 100f, "we should report 100% progress.");
    Assert.assertNotNull(reader.get(), "Expected one result.");
    Assert.assertEquals(reader.get(), mockRow, "Reader should return the mocked InternalRow."); //
    Assert.assertFalse(reader.next(), "Reader should NOT have any more messages available.");

    Assert.assertEquals(reader.readerStats.getRecordsSkipped(), 1, "ReaderStats report one skipped control message");

    reader.close();
  }

  // test to make sure getStats() returns the expected values
  @Test
  public void testGetStats() {
    String region = "ei-test-region3";

    // updated constants to adjust the test behavior and timeouts
    final int CONSUMER_POLL_EMPTY_RESULT_RETRY_TIMES = 3;
    final long EMPTY_POLL_SLEEP_TIME_MS = TimeUnit.SECONDS.toMillis(1);
    final long CONSUMER_POLL_TIMEOUT = TimeUnit.SECONDS.toMillis(1); // 1 second

    reader = new SparkPubSubInputPartitionReader(
        mockInputPartition,
        mockConsumer,
        topicPartition,
        region,
        false,
        CONSUMER_POLL_TIMEOUT,
        CONSUMER_POLL_EMPTY_RESULT_RETRY_TIMES,
        EMPTY_POLL_SLEEP_TIME_MS,
        mockMessageToRowConverter);

    // Assert that stats are initialized to zero
    Assert.assertEquals(reader.readerStats.getRecordsServed(), 0, "Records served should be zero initially.");
    Assert.assertEquals(reader.readerStats.getRecordsSkipped(), 0, "Records skipped should be zero initially.");
    Assert.assertEquals(
        reader.readerStats.getRecordsDeliveredByGet(),
        0,
        "Records delivered by get should be zero initially.");
  }

  /**
  * Helper method to create a mock PubSub message with specified offset and control flag
  * @param offset The offset for the message
  * @param isControlMessage Whether this message should be a control message
  * @return Mocked DefaultPubSubMessage
  */
  private DefaultPubSubMessage createMockMessage(long offset, boolean isControlMessage) {
    DefaultPubSubMessage mockMessage = Mockito.mock(DefaultPubSubMessage.class);
    KafkaKey mockKey = Mockito.mock(KafkaKey.class);
    when(mockKey.isControlMessage()).thenReturn(isControlMessage);
    PubSubPosition mockPosition = ApacheKafkaOffsetPosition.of(offset);
    when(mockMessage.getOffset()).thenReturn(mockPosition);
    when(mockMessage.getPosition()).thenReturn(mockPosition);
    when(mockMessage.getKey()).thenReturn(mockKey);
    return mockMessage;
  }

  /**
  * Helper method to create a poll result map for a single message
  * @param topicPartition The topic partition
  * @param message The message to include in the result
  * @return A map containing the topic partition and message
  */
  private Map<PubSubTopicPartition, List<DefaultPubSubMessage>> createPollResultMap(
      PubSubTopicPartition topicPartition,
      DefaultPubSubMessage message) {
    Map<PubSubTopicPartition, List<DefaultPubSubMessage>> pollResult = new HashMap<>();
    pollResult.put(topicPartition, Collections.singletonList(message));
    return pollResult;
  }
}
