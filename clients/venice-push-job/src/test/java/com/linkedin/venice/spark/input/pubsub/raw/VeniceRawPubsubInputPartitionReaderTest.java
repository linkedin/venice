package com.linkedin.venice.spark.input.pubsub.raw;

import static org.mockito.Mockito.*;

import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.spark.input.pubsub.VenicePubSubMessageToRow;
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
 * Unit tests for {@link VeniceRawPubsubInputPartitionReader}.
 *
 * This test class validates the functionality of the partition reader including:
 * - Reading messages from PubSub topic partition
 * - Handling of starting and ending offsets
 * - Conversion of PubSub messages to Spark InternalRow format
 * - Filtering of control messages
 * - Progress tracking and reporting
 * - Error conditions and edge cases
 */
public class VeniceRawPubsubInputPartitionReaderTest {
  @Mock
  private VeniceBasicPubsubInputPartition mockInputPartition;

  @Mock
  private PubSubConsumerAdapter mockConsumer;

  @Mock
  private PubSubTopic mockTopic;

  @Mock
  private PubSubTopicPartition mockTopicPartition;

  @Mock
  private VenicePubSubMessageToRow mockMessageToRowConverter;

  private VeniceRawPubsubInputPartitionReader reader;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    // Setup will be implemented
  }

  @AfterMethod
  public void tearDown() {
    if (reader != null) {
      reader.close();
    }
  }

  @Test
  public void testEmptyTopicInitialization() {
    // Arrange
    int partitionNumber = 5;
    String topicName = "test-topic";
    String region = "test-region";
    long startOffset = 101L;
    long endOffset = 200L;

    // updated constants to adjust the test behavior and timeouts
    final int CONSUMER_POLL_EMPTY_RESULT_RETRY_TIMES = 3;
    final long EMPTY_POLL_SLEEP_TIME_MS = TimeUnit.SECONDS.toMillis(1);
    final long CONSUMER_POLL_TIMEOUT = TimeUnit.SECONDS.toMillis(1); // 1 second

    // Setup mocks
    when(mockInputPartition.getPartitionNumber()).thenReturn(partitionNumber);
    when(mockInputPartition.getTopicName()).thenReturn(topicName);
    when(mockInputPartition.getRegion()).thenReturn(region);
    when(mockInputPartition.getStartOffset()).thenReturn(startOffset);
    when(mockInputPartition.getEndOffset()).thenReturn(endOffset);
    when(mockTopicPartition.getPubSubTopic()).thenReturn(mockTopic);

    // Act
    long start = System.currentTimeMillis();
    reader = new VeniceRawPubsubInputPartitionReader(
        mockInputPartition,
        mockConsumer,
        mockTopicPartition,
        CONSUMER_POLL_TIMEOUT,
        CONSUMER_POLL_EMPTY_RESULT_RETRY_TIMES,
        EMPTY_POLL_SLEEP_TIME_MS,
        new VenicePubSubMessageToRow());

    Assert.assertFalse(reader.next()); // Attempt to read messages, which should fail due to empty topic

    long elapsed = System.currentTimeMillis() - start;
    Assert.assertTrue(elapsed >= 3000, "Constructor should take at least 3 seconds due to polling retries.");
    Assert.assertTrue(elapsed < 4000, "Constructor should not exceed 4 seconds, it doesn't do much after failure.");

    reader.close();
  }

  @Test
  public void testShortTopicConsumeConvert() {
    // Arrange
    int partitionNumber = 5;
    String topicName = "test-topic";
    String region = "test-region";
    long startOffset = 1L;
    long endOffset = 2L;

    // updated constants to adjust the test behavior and timeouts
    final int CONSUMER_POLL_EMPTY_RESULT_RETRY_TIMES = 3;
    final long EMPTY_POLL_SLEEP_TIME_MS = TimeUnit.SECONDS.toMillis(1);
    final long CONSUMER_POLL_TIMEOUT = TimeUnit.SECONDS.toMillis(1); // 1 second

    // Setup mocks
    when(mockInputPartition.getPartitionNumber()).thenReturn(partitionNumber);
    when(mockInputPartition.getTopicName()).thenReturn(topicName);
    when(mockInputPartition.getRegion()).thenReturn(region);
    when(mockInputPartition.getStartOffset()).thenReturn(startOffset);
    when(mockInputPartition.getEndOffset()).thenReturn(endOffset);
    when(mockTopicPartition.getPubSubTopic()).thenReturn(mockTopic);

    KafkaKey mockKey1 = Mockito.mock(KafkaKey.class);
    when(mockKey1.isControlMessage()).thenReturn(false);
    PubSubPosition mockPosition1 = ApacheKafkaOffsetPosition.of(startOffset);

    DefaultPubSubMessage mockMessage1 = Mockito.mock(DefaultPubSubMessage.class);
    when(mockMessage1.getOffset()).thenReturn(mockPosition1);
    when(mockMessage1.getKey()).thenReturn(mockKey1); // No key for this message

    KafkaKey mockKey2 = Mockito.mock(KafkaKey.class);
    when(mockKey2.isControlMessage()).thenReturn(false);
    DefaultPubSubMessage mockMessage2 = Mockito.mock(DefaultPubSubMessage.class);
    PubSubPosition mockPosition2 = ApacheKafkaOffsetPosition.of(startOffset + 1L);
    when(mockMessage2.getOffset()).thenReturn(mockPosition2);
    when(mockMessage2.getKey()).thenReturn(mockKey2);

    InternalRow mockRow = Mockito.mock(InternalRow.class);

    // Mock the poll function to return the mock message
    Map<PubSubTopicPartition, List<DefaultPubSubMessage>> pollResult1 = new HashMap<>();
    pollResult1.put(mockTopicPartition, Collections.singletonList(mockMessage1));

    Map<PubSubTopicPartition, List<DefaultPubSubMessage>> pollResult2 = new HashMap<>();
    pollResult2.put(mockTopicPartition, Collections.singletonList(mockMessage2));

    when(mockConsumer.poll(CONSUMER_POLL_TIMEOUT)).thenReturn(pollResult1)
        .thenReturn(pollResult2)
        .thenReturn(new HashMap<>());
    // mock the return value of the message converter

    when(mockMessageToRowConverter.convert(Mockito.eq(mockMessage1), Mockito.eq(region), Mockito.eq(partitionNumber)))
        .thenReturn(mockRow);

    when(mockMessageToRowConverter.convert(Mockito.eq(mockMessage2), Mockito.eq(region), Mockito.eq(partitionNumber)))
        .thenReturn(mockRow);

    reader = new VeniceRawPubsubInputPartitionReader(
        mockInputPartition,
        mockConsumer,
        mockTopicPartition,
        CONSUMER_POLL_TIMEOUT,
        CONSUMER_POLL_EMPTY_RESULT_RETRY_TIMES,
        EMPTY_POLL_SLEEP_TIME_MS,
        mockMessageToRowConverter);

    // to inspect the state of the reader

    Assert.assertTrue(reader.next(), "Reader should have a next message available");
    Assert.assertNotNull(reader.get(), "Expected one result");
    Assert.assertEquals(reader.get(), mockRow, "Reader should return the expected InternalRow");
    Assert.assertTrue(reader.next(), "Reader should have a next message available");
    Assert.assertFalse(reader.next(), "Reader should have a next message available");

    // at this point, we keep returning the last good row, and there wont be any "next" message available.
    Assert.assertFalse(reader.next(), "Reader should have a next message available");
    Assert.assertEquals(reader.get(), mockRow, "Reader should return the expected InternalRow");

    reader.close();
  }

  @Test
  public void testFilterControlMessages() {
    // Arrange
    int partitionNumber = 5;
    String topicName = "test-topic";
    String region = "test-region";
    long startOffset = 1L;
    long endOffset = 2L;

    // updated constants to adjust the test behavior and timeouts
    final int CONSUMER_POLL_EMPTY_RESULT_RETRY_TIMES = 3;
    final long EMPTY_POLL_SLEEP_TIME_MS = TimeUnit.SECONDS.toMillis(1);
    final long CONSUMER_POLL_TIMEOUT = TimeUnit.SECONDS.toMillis(1); // 1 second

    // Setup mocks
    when(mockInputPartition.getPartitionNumber()).thenReturn(partitionNumber);
    when(mockInputPartition.getTopicName()).thenReturn(topicName);
    when(mockInputPartition.getRegion()).thenReturn(region);
    when(mockInputPartition.getStartOffset()).thenReturn(startOffset);
    when(mockInputPartition.getEndOffset()).thenReturn(endOffset);
    when(mockTopicPartition.getPubSubTopic()).thenReturn(mockTopic);

    KafkaKey mockKey1 = Mockito.mock(KafkaKey.class);
    when(mockKey1.isControlMessage()).thenReturn(true);
    PubSubPosition mockPosition1 = ApacheKafkaOffsetPosition.of(startOffset);

    DefaultPubSubMessage mockMessage1 = Mockito.mock(DefaultPubSubMessage.class);
    when(mockKey1.isControlMessage()).thenReturn(true);
    when(mockMessage1.getOffset()).thenReturn(mockPosition1);
    when(mockMessage1.getKey()).thenReturn(mockKey1); // No key for this message

    KafkaKey mockKey2 = Mockito.mock(KafkaKey.class);
    when(mockKey2.isControlMessage()).thenReturn(false);
    DefaultPubSubMessage mockMessage2 = Mockito.mock(DefaultPubSubMessage.class);
    PubSubPosition mockPosition2 = ApacheKafkaOffsetPosition.of(startOffset + 1L);
    when(mockMessage2.getOffset()).thenReturn(mockPosition2);
    when(mockMessage2.getKey()).thenReturn(mockKey2);

    InternalRow mockRow = Mockito.mock(InternalRow.class);

    // Mock the poll function to return the mock message
    Map<PubSubTopicPartition, List<DefaultPubSubMessage>> pollResult1 = new HashMap<>();
    pollResult1.put(mockTopicPartition, Collections.singletonList(mockMessage1));

    Map<PubSubTopicPartition, List<DefaultPubSubMessage>> pollResult2 = new HashMap<>();
    pollResult2.put(mockTopicPartition, Collections.singletonList(mockMessage2));

    when(mockConsumer.poll(CONSUMER_POLL_TIMEOUT)).thenReturn(pollResult1)
        .thenReturn(pollResult2)
        .thenReturn(new HashMap<>());
    // mock the return value of the message converter

    when(mockMessageToRowConverter.convert(Mockito.eq(mockMessage1), Mockito.eq(region), Mockito.eq(partitionNumber)))
        .thenReturn(mockRow);

    when(mockMessageToRowConverter.convert(Mockito.eq(mockMessage2), Mockito.eq(region), Mockito.eq(partitionNumber)))
        .thenReturn(mockRow);

    reader = new VeniceRawPubsubInputPartitionReader(
        mockInputPartition,
        mockConsumer,
        mockTopicPartition,
        CONSUMER_POLL_TIMEOUT,
        CONSUMER_POLL_EMPTY_RESULT_RETRY_TIMES,
        EMPTY_POLL_SLEEP_TIME_MS,
        mockMessageToRowConverter);

    // to inspect the state of the reader

    Assert.assertTrue(reader.next(), "Reader should have a next message available");
    Assert.assertNotNull(reader.get(), "Expected one result");
    Assert.assertEquals(reader.get(), mockRow, "Reader should return the expected InternalRow");
    Assert.assertFalse(reader.next(), "Reader should have a next message available");
    // assert that skipped records is 1
    Assert.assertEquals(reader.readerStats.getRecordsSkipped(), 1, "Reader should have skipped one control message");
    // at this point, we keep returning the last good row, and there wont be any "next" message available.
    Assert.assertFalse(reader.next(), "Reader should have a next message available");
    Assert.assertEquals(reader.get(), mockRow, "Reader should return the expected InternalRow");

    reader.close();
  }

  // test to make sure getStats() returns the expected values
  @Test
  public void testGetStats() {

    // updated constants to adjust the test behavior and timeouts
    final int CONSUMER_POLL_EMPTY_RESULT_RETRY_TIMES = 3;
    final long EMPTY_POLL_SLEEP_TIME_MS = TimeUnit.SECONDS.toMillis(1);
    final long CONSUMER_POLL_TIMEOUT = TimeUnit.SECONDS.toMillis(1); // 1 second

    reader = new VeniceRawPubsubInputPartitionReader(
        mockInputPartition,
        mockConsumer,
        mockTopicPartition,
        CONSUMER_POLL_TIMEOUT,
        CONSUMER_POLL_EMPTY_RESULT_RETRY_TIMES,
        EMPTY_POLL_SLEEP_TIME_MS,
        mockMessageToRowConverter);

    // Assert that stats are initialized to zero
    Assert.assertEquals(reader.readerStats.getRecordsServed(), 0, "Records served should be zero initially");
    Assert.assertEquals(reader.readerStats.getRecordsSkipped(), 0, "Records skipped should be zero initially");
    Assert.assertEquals(
        reader.readerStats.getRecordsDeliveredByGet(),
        0,
        "Records delivered by get should be zero initially");
  }

}
