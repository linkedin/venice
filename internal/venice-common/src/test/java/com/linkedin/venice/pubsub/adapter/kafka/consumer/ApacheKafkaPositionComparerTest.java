package com.linkedin.venice.pubsub.adapter.kafka.consumer;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubSymbolicPosition;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ApacheKafkaPositionComparerTest {
  private ApacheKafkaPositionComparer comparer;
  private PubSubTopicPartition topicPartition;

  @BeforeMethod
  public void setUp() {
    comparer = ApacheKafkaPositionComparer.INSTANCE;
    PubSubTopicRepository topicRepository = new PubSubTopicRepository();
    topicPartition = new PubSubTopicPartitionImpl(topicRepository.getTopic("test_v1"), 0);
  }

  @Test
  public void testCompareEqualPositions() {
    PubSubPosition pos10 = new ApacheKafkaOffsetPosition(10L);
    assertEquals(comparer.comparePositions(topicPartition, pos10, pos10), 0L);
  }

  @Test
  public void testComparePositionsBefore() {
    PubSubPosition pos10 = new ApacheKafkaOffsetPosition(10L);
    PubSubPosition pos20 = new ApacheKafkaOffsetPosition(20L);
    assertTrue(comparer.comparePositions(topicPartition, pos10, pos20) < 0);
  }

  @Test
  public void testComparePositionsAfter() {
    PubSubPosition pos10 = new ApacheKafkaOffsetPosition(10L);
    PubSubPosition pos20 = new ApacheKafkaOffsetPosition(20L);
    assertTrue(comparer.comparePositions(topicPartition, pos20, pos10) > 0);
  }

  @Test
  public void testRejectsNullPosition1() {
    PubSubPosition pos10 = new ApacheKafkaOffsetPosition(10L);
    expectThrows(IllegalArgumentException.class, () -> comparer.comparePositions(topicPartition, null, pos10));
  }

  @Test
  public void testRejectsNullPosition2() {
    PubSubPosition pos10 = new ApacheKafkaOffsetPosition(10L);
    expectThrows(IllegalArgumentException.class, () -> comparer.comparePositions(topicPartition, pos10, null));
  }

  @Test
  public void testRejectsSymbolicEarliest() {
    PubSubPosition pos10 = new ApacheKafkaOffsetPosition(10L);
    expectThrows(
        IllegalArgumentException.class,
        () -> comparer.comparePositions(topicPartition, PubSubSymbolicPosition.EARLIEST, pos10));
  }

  @Test
  public void testRejectsSymbolicLatest() {
    PubSubPosition pos10 = new ApacheKafkaOffsetPosition(10L);
    expectThrows(
        IllegalArgumentException.class,
        () -> comparer.comparePositions(topicPartition, pos10, PubSubSymbolicPosition.LATEST));
  }

  @Test
  public void testRejectsBothSymbolic() {
    expectThrows(
        IllegalArgumentException.class,
        () -> comparer
            .comparePositions(topicPartition, PubSubSymbolicPosition.EARLIEST, PubSubSymbolicPosition.LATEST));
  }

  @Test
  public void testSingletonInstance() {
    assertEquals(ApacheKafkaPositionComparer.INSTANCE, comparer);
  }
}
