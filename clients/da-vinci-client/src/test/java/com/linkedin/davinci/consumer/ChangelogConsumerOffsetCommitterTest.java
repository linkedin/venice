package com.linkedin.davinci.consumer;

import static com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory.FAIL;
import static com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory.SUCCESS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.consumer.stats.BasicConsumerStats;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubSymbolicPosition;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.api.exceptions.PubSubClientException;
import com.linkedin.venice.utils.TestMockTime;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ChangelogConsumerOffsetCommitterTest {
  private static final long INTERVAL_MS = 60_000L;
  private static final long TIMEOUT_MS = 5_000L;
  private static final Duration TIMEOUT = Duration.ofMillis(TIMEOUT_MS);

  private final PubSubTopicRepository topicRepository = new PubSubTopicRepository();
  private final PubSubTopicPartition v1p0 = new PubSubTopicPartitionImpl(topicRepository.getTopic("store_v1"), 0);
  private final PubSubTopicPartition v1p1 = new PubSubTopicPartitionImpl(topicRepository.getTopic("store_v1"), 1);
  private final PubSubTopicPartition v2p0 = new PubSubTopicPartitionImpl(topicRepository.getTopic("store_v2"), 0);

  private PubSubConsumerAdapter adapter;
  private BasicConsumerStats stats;
  private TestMockTime time;
  private ChangelogConsumerOffsetCommitter committer;

  @BeforeMethod
  public void setUp() {
    adapter = mock(PubSubConsumerAdapter.class);
    stats = mock(BasicConsumerStats.class);
    time = new TestMockTime(1_000L);
    committer = new ChangelogConsumerOffsetCommitter(adapter, INTERVAL_MS, TIMEOUT_MS, time, stats, "test-consumer");
  }

  private static PubSubPosition pos(long offset) {
    return ApacheKafkaOffsetPosition.of(offset);
  }

  @Test
  public void testMaybeCommitIsIntervalGated() {
    committer.recordConsumed(v1p0, pos(10));
    committer.maybeCommit();
    verify(adapter, never()).commitSync(anyMap(), any());

    time.addMilliseconds(INTERVAL_MS - 1);
    committer.maybeCommit();
    verify(adapter, never()).commitSync(anyMap(), any());

    time.addMilliseconds(1);
    committer.maybeCommit();
    verify(adapter).commitSync(Collections.singletonMap(v1p0, pos(10)), TIMEOUT);
    verify(stats).emitOffsetCommitCountMetrics(SUCCESS);

    // The interval restarts after each commit.
    committer.maybeCommit();
    verify(adapter, times(1)).commitSync(anyMap(), any());
  }

  @Test
  public void testUnchangedPositionsAreRecommittedEachInterval() {
    committer.recordConsumed(v1p0, pos(10));
    committer.commit();
    time.addMilliseconds(INTERVAL_MS);
    committer.maybeCommit();
    verify(adapter, times(2)).commitSync(Collections.singletonMap(v1p0, pos(10)), TIMEOUT);
  }

  @Test
  public void testNothingToCommitSkipsAdapter() {
    committer.commit();
    verify(adapter, never()).commitSync(anyMap(), any());
    verify(stats, never()).emitOffsetCommitCountMetrics(any());
  }

  @Test
  public void testLatestPositionPerTopicPartitionWins() {
    committer.recordConsumed(v1p0, pos(1));
    committer.recordConsumed(v1p0, pos(2));
    committer.recordConsumed(v1p1, pos(7));
    committer.commit();
    Map<PubSubTopicPartition, PubSubPosition> expected = new HashMap<>();
    expected.put(v1p0, pos(2));
    expected.put(v1p1, pos(7));
    verify(adapter).commitSync(expected, TIMEOUT);
  }

  @Test
  public void testNullAndSymbolicPositionsAreIgnored() {
    committer.recordConsumed(v1p0, pos(3));
    committer.recordConsumed(v1p0, PubSubSymbolicPosition.LATEST);
    committer.recordConsumed(v1p0, null);
    committer.recordConsumed(null, pos(9));
    assertEquals(committer.getLatestPositions(), Collections.singletonMap(v1p0, pos(3)));
  }

  @Test
  public void testTopicChangeRetiresPreviousTopicForOneCommit() {
    committer.recordConsumed(v1p0, pos(100));
    committer.recordConsumed(v2p0, pos(5));
    assertEquals(committer.getRetiringPositions(), Collections.singletonMap(v1p0, pos(100)));
    assertEquals(committer.getLatestPositions(), Collections.singletonMap(v2p0, pos(5)));

    committer.commit();
    Map<PubSubTopicPartition, PubSubPosition> expected = new HashMap<>();
    expected.put(v1p0, pos(100));
    expected.put(v2p0, pos(5));
    verify(adapter).commitSync(expected, TIMEOUT);
    assertTrue(committer.getRetiringPositions().isEmpty());

    committer.commit();
    verify(adapter).commitSync(Collections.singletonMap(v2p0, pos(5)), TIMEOUT);
  }

  @Test
  public void testRetiringPositionsAreDroppedEvenIfCommitFails() {
    // A deleted old version topic must not make every later commit fail.
    committer.recordConsumed(v1p0, pos(100));
    committer.recordConsumed(v2p0, pos(5));
    doThrow(new PubSubClientException("unknown topic")).when(adapter).commitSync(anyMap(), any());
    committer.commit();
    assertTrue(committer.getRetiringPositions().isEmpty());

    reset(adapter);
    committer.commit();
    verify(adapter).commitSync(Collections.singletonMap(v2p0, pos(5)), TIMEOUT);
  }

  @Test
  public void testReturningToRetiredTopicCancelsRetirement() {
    committer.recordConsumed(v1p0, pos(1));
    committer.recordConsumed(v2p0, pos(2));
    committer.recordConsumed(v1p0, pos(3));
    assertEquals(committer.getLatestPositions(), Collections.singletonMap(v1p0, pos(3)));
    assertEquals(committer.getRetiringPositions(), Collections.singletonMap(v2p0, pos(2)));
  }

  @Test
  public void testRemoveSpecificPartitions() {
    committer.recordConsumed(v1p0, pos(1));
    committer.recordConsumed(v1p1, pos(2));
    committer.removePartitions(Collections.singleton(1));
    assertEquals(committer.getLatestPositions(), Collections.singletonMap(v1p0, pos(1)));
    assertEquals(committer.getRetiringPositions(), Collections.singletonMap(v1p1, pos(2)));

    committer.commit();
    Map<PubSubTopicPartition, PubSubPosition> both = new HashMap<>();
    both.put(v1p0, pos(1));
    both.put(v1p1, pos(2));
    verify(adapter).commitSync(both, TIMEOUT);

    committer.commit();
    verify(adapter).commitSync(Collections.singletonMap(v1p0, pos(1)), TIMEOUT);
    verify(adapter, times(2)).commitSync(anyMap(), any());
  }

  @Test
  public void testRemoveAllPartitionsWithEmptySet() {
    committer.recordConsumed(v1p0, pos(1));
    committer.recordConsumed(v1p1, pos(2));
    committer.removePartitions(Collections.emptySet());
    assertTrue(committer.getLatestPositions().isEmpty());
    assertEquals(committer.getRetiringPositions().size(), 2);

    committer.commit();
    Map<PubSubTopicPartition, PubSubPosition> both = new HashMap<>();
    both.put(v1p0, pos(1));
    both.put(v1p1, pos(2));
    verify(adapter).commitSync(both, TIMEOUT);
    assertTrue(committer.getRetiringPositions().isEmpty());
  }

  @Test
  public void testCommitFailureIsSwallowedAndCounted() {
    committer.recordConsumed(v1p0, pos(1));
    doThrow(new PubSubClientException("broker down")).when(adapter).commitSync(anyMap(), any());
    committer.commit();
    committer.commit();
    verify(stats, times(2)).emitOffsetCommitCountMetrics(FAIL);
    verify(stats, never()).emitOffsetCommitCountMetrics(SUCCESS);
    assertFalse(committer.isDisabled());
    assertEquals(committer.getLatestPositions(), Collections.singletonMap(v1p0, pos(1)));
  }

  @Test
  public void testFailedCommitStillRestartsInterval() {
    committer.recordConsumed(v1p0, pos(1));
    doThrow(new RuntimeException("boom")).when(adapter).commitSync(anyMap(), any());
    time.addMilliseconds(INTERVAL_MS);
    committer.maybeCommit();
    committer.maybeCommit();
    verify(adapter, times(1)).commitSync(anyMap(), any());
  }

  @Test
  public void testUnsupportedAdapterDisablesCommitter() {
    committer.recordConsumed(v1p0, pos(1));
    doThrow(new UnsupportedOperationException()).when(adapter).commitSync(anyMap(), any());
    committer.commit();
    assertTrue(committer.isDisabled());
    assertTrue(committer.getLatestPositions().isEmpty());

    committer.recordConsumed(v1p0, pos(2));
    committer.commit();
    verify(adapter, times(1)).commitSync(anyMap(), any());
    assertTrue(committer.getLatestPositions().isEmpty());
    verify(stats, never()).emitOffsetCommitCountMetrics(any());
  }

  @Test
  public void testNullStatsIsAllowed() {
    ChangelogConsumerOffsetCommitter noStats =
        new ChangelogConsumerOffsetCommitter(adapter, INTERVAL_MS, TIMEOUT_MS, time, null, "test-consumer");
    noStats.recordConsumed(v1p0, pos(1));
    noStats.commit();
    verify(adapter).commitSync(Collections.singletonMap(v1p0, pos(1)), TIMEOUT);
  }
}
