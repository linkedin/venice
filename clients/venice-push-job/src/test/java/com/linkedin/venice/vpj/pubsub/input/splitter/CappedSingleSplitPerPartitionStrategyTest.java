package com.linkedin.venice.vpj.pubsub.input.splitter;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.vpj.pubsub.input.PartitionSplitStrategy;
import com.linkedin.venice.vpj.pubsub.input.PubSubPartitionSplit;
import com.linkedin.venice.vpj.pubsub.input.SplitRequest;
import java.util.List;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class CappedSingleSplitPerPartitionStrategyTest {
  @DataProvider
  public Object[][] partitionSizes() {
    return new Object[][] { { 0L, 0L }, { 3L, 3L }, { 10L, 10L }, { Long.MAX_VALUE, 10L } };
  }

  @Test(dataProvider = "partitionSizes")
  public void testOnlyOneBoundedPrefixIsPlanned(long partitionSize, long expectedCount) {
    PubSubTopicRepository repository = new PubSubTopicRepository();
    PubSubTopicPartition partition = new PubSubTopicPartitionImpl(repository.getTopic("test_topic"), 3);
    TopicManager manager = mock(TopicManager.class);
    when(manager.getTopicRepository()).thenReturn(repository);
    PubSubPosition start = ApacheKafkaOffsetPosition.of(100);
    PubSubPosition end = ApacheKafkaOffsetPosition.of(100 + expectedCount);
    when(manager.advancePosition(partition, start, expectedCount)).thenReturn(end);
    SplitRequest.Builder builder = new SplitRequest.Builder().topicManager(manager)
        .pubSubTopicPartition(partition)
        .splitType(PartitionSplitStrategy.CAPPED_SINGLE_SPLIT_PER_PARTITION)
        .startPosition(start)
        .endPosition(ApacheKafkaOffsetPosition.of(Long.MAX_VALUE))
        .numberOfRecords(partitionSize);
    expectThrows(IllegalArgumentException.class, builder::build);
    expectThrows(IllegalArgumentException.class, () -> builder.recordsPerSplit(-1).build());
    expectThrows(IllegalArgumentException.class, () -> builder.recordsPerSplit(0).build());
    List<PubSubPartitionSplit> splits =
        PartitionSplitStrategy.CAPPED_SINGLE_SPLIT_PER_PARTITION.split(builder.recordsPerSplit(10).build());

    assertEquals(splits.size(), 1);
    PubSubPartitionSplit split = splits.get(0);
    assertEquals(split.getStartPubSubPosition(), start);
    assertEquals(split.getEndPubSubPosition(), end);
    assertEquals(split.getNumberOfRecords(), expectedCount);
    verify(manager, expectedCount == 0 ? never() : times(1)).advancePosition(partition, start, expectedCount);
  }
}
