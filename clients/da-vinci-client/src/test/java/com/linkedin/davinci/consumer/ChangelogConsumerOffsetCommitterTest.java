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

import com.linkedin.davinci.consumer.stats.BasicConsumerStats;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubSymbolicPosition;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.api.exceptions.PubSubClientException;
import com.linkedin.venice.utils.DataProviderUtils;
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

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testCommitIntervalRetainsLatestConcretePositions(boolean failCommit) {
    committer.commit();
    verify(adapter, never()).commitSync(anyMap(), any());
    verify(stats, never()).emitOffsetCommitCountMetrics(any());
    if (failCommit) {
      doThrow(new PubSubClientException("broker down")).when(adapter).commitSync(anyMap(), any());
    }
    committer.recordConsumed(v1p0, pos(1));
    committer.recordConsumed(v1p0, pos(10));
    committer.recordConsumed(v1p1, pos(7));
    committer.recordConsumed(v1p0, PubSubSymbolicPosition.LATEST);
    committer.recordConsumed(v1p0, null);
    committer.recordConsumed(null, pos(9));
    committer.maybeCommit();
    verify(adapter, never()).commitSync(anyMap(), any());
    time.addMilliseconds(INTERVAL_MS - 1);
    committer.maybeCommit();
    verify(adapter, never()).commitSync(anyMap(), any());

    Map<PubSubTopicPartition, PubSubPosition> expected = new HashMap<>();
    expected.put(v1p0, pos(10));
    expected.put(v1p1, pos(7));
    time.addMilliseconds(1);
    committer.maybeCommit();
    verify(adapter).commitSync(expected, TIMEOUT);
    committer.maybeCommit();
    verify(adapter).commitSync(anyMap(), any());
    time.addMilliseconds(INTERVAL_MS);
    committer.maybeCommit();
    verify(adapter, times(2)).commitSync(expected, TIMEOUT);
    verify(stats, times(2)).emitOffsetCommitCountMetrics(failCommit ? FAIL : SUCCESS);
    verify(stats, never()).emitOffsetCommitCountMetrics(failCommit ? SUCCESS : FAIL);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testTopicChangeRetiresPositionsAfterOneAttempt(boolean failCommit) {
    committer.recordConsumed(v1p0, pos(100));
    committer.recordConsumed(v2p0, pos(5));
    if (failCommit) {
      doThrow(new PubSubClientException("unknown topic")).when(adapter).commitSync(anyMap(), any());
    }
    committer.commit();
    Map<PubSubTopicPartition, PubSubPosition> expected = new HashMap<>();
    expected.put(v1p0, pos(100));
    expected.put(v2p0, pos(5));
    verify(adapter).commitSync(expected, TIMEOUT);
    reset(adapter);
    committer.commit();
    verify(adapter).commitSync(Collections.singletonMap(v2p0, pos(5)), TIMEOUT);
  }

  @Test
  public void testReturningToRetiredTopicCancelsRetirement() {
    committer.recordConsumed(v1p0, pos(1));
    committer.recordConsumed(v2p0, pos(2));
    committer.recordConsumed(v1p0, pos(3));
    committer.commit();
    Map<PubSubTopicPartition, PubSubPosition> expected = new HashMap<>();
    expected.put(v1p0, pos(3));
    expected.put(v2p0, pos(2));
    verify(adapter).commitSync(expected, TIMEOUT);
    committer.commit();
    verify(adapter).commitSync(Collections.singletonMap(v1p0, pos(3)), TIMEOUT);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testRemovedPartitionsAreCommittedOnlyOnce(boolean removeAll) {
    committer.recordConsumed(v1p0, pos(1));
    committer.recordConsumed(v1p1, pos(2));
    committer.removePartitions(removeAll ? Collections.emptySet() : Collections.singleton(1));
    committer.commit();
    Map<PubSubTopicPartition, PubSubPosition> both = new HashMap<>();
    both.put(v1p0, pos(1));
    both.put(v1p1, pos(2));
    verify(adapter).commitSync(both, TIMEOUT);
    committer.commit();
    if (!removeAll) {
      verify(adapter).commitSync(Collections.singletonMap(v1p0, pos(1)), TIMEOUT);
    }
    verify(adapter, times(removeAll ? 1 : 2)).commitSync(anyMap(), any());
  }

  @Test
  public void testUnsupportedAdapterDisablesCommitter() {
    committer.recordConsumed(v1p0, pos(1));
    committer.recordConsumed(v2p0, pos(2));
    doThrow(new UnsupportedOperationException()).when(adapter).commitSync(anyMap(), any());
    committer.commit();
    committer.recordConsumed(v1p0, pos(2));
    committer.commit();
    verify(adapter, times(1)).commitSync(anyMap(), any());
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
