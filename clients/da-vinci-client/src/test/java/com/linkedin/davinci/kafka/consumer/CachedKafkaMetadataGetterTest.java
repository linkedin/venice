package com.linkedin.davinci.kafka.consumer;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.TopicDoesNotExistException;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.stats.StatsErrorCode;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.testng.Assert;
import org.testng.annotations.Test;


public class CachedKafkaMetadataGetterTest {
  private final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();

  @Test
  public void testGetEarliestOffset() {
    CachedKafkaMetadataGetter cachedKafkaMetadataGetter = new CachedKafkaMetadataGetter(1000);
    TopicManager mockTopicManager = mock(TopicManager.class);
    PubSubTopic testTopic = pubSubTopicRepository.getTopic("test_v1");
    int partition = 0;
    PubSubTopicPartition testTopicPartition = new PubSubTopicPartitionImpl(testTopic, partition);
    String testBrokerUrl = "I_Am_A_Broker_dot_com.com";
    Long earliestOffset = 1L;
    when(mockTopicManager.getKafkaBootstrapServers()).thenReturn(testBrokerUrl);
    when(mockTopicManager.getPartitionEarliestOffsetAndRetry(any(), anyInt())).thenReturn(earliestOffset);
    Assert.assertEquals(
        (Long) cachedKafkaMetadataGetter.getEarliestOffset(mockTopicManager, testTopicPartition),
        earliestOffset);

    TopicManager mockTopicManagerThatThrowsException = mock(TopicManager.class);
    when(mockTopicManagerThatThrowsException.getKafkaBootstrapServers()).thenReturn(testBrokerUrl);
    when(mockTopicManagerThatThrowsException.getPartitionEarliestOffsetAndRetry(any(), anyInt()))
        .thenThrow(TopicDoesNotExistException.class);

    // Even though we're passing a weird topic manager, we should have cached the last value, so this should return the
    // cached offset of 1
    Assert.assertEquals(
        (Long) cachedKafkaMetadataGetter.getEarliestOffset(mockTopicManagerThatThrowsException, testTopicPartition),
        earliestOffset);

    // Now check for an uncached value and verify we get the error code for topic does not exist.
    Assert.assertEquals(
        cachedKafkaMetadataGetter.getEarliestOffset(
            mockTopicManagerThatThrowsException,
            new PubSubTopicPartitionImpl(testTopic, partition + 1)),
        StatsErrorCode.LAG_MEASUREMENT_FAILURE.code);

  }

  @Test
  public void testCacheWillResetStatusWhenExceptionIsThrown() {
    CachedKafkaMetadataGetter cachedKafkaMetadataGetter = new CachedKafkaMetadataGetter(1000);
    CachedKafkaMetadataGetter.KafkaMetadataCacheKey key = new CachedKafkaMetadataGetter.KafkaMetadataCacheKey(
        "server",
        new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic(TestUtils.getUniqueTopicString("topic")), 1));
    Map<CachedKafkaMetadataGetter.KafkaMetadataCacheKey, CachedKafkaMetadataGetter.ValueAndExpiryTime<Long>> offsetCache =
        new VeniceConcurrentHashMap<>();
    CachedKafkaMetadataGetter.ValueAndExpiryTime<Long> valueCache =
        new CachedKafkaMetadataGetter.ValueAndExpiryTime<>(1L, System.nanoTime());
    offsetCache.put(key, valueCache);
    // Successful call will update the value from 1 to 2.
    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      Long actualResult = cachedKafkaMetadataGetter.fetchMetadata(key, offsetCache, () -> 2L);
      Long expectedResult = 2L;
      Assert.assertEquals(actualResult, expectedResult);
    });

    // For persisting exception, it will be caught and thrown.
    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      Assert.assertThrows(VeniceException.class, () -> cachedKafkaMetadataGetter.fetchMetadata(key, offsetCache, () -> {
        throw new VeniceException("dummy exception");
      }));
    });

    // Reset the cached value to 1.
    valueCache = new CachedKafkaMetadataGetter.ValueAndExpiryTime<>(1L, System.nanoTime());
    offsetCache.put(key, valueCache);

    // The first call throws a transient exception, and it should be updated to expected value after second call.
    AtomicBoolean exceptionFlag = new AtomicBoolean(false);
    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, false, true, () -> {
      Long actualResult = cachedKafkaMetadataGetter.fetchMetadata(key, offsetCache, () -> {
        if (exceptionFlag.compareAndSet(false, true)) {
          throw new VeniceException("do not throw this exception!");
        } else {
          return 2L;
        }
      });
      Long expectedResult = 2L;
      Assert.assertEquals(actualResult, expectedResult);
    });
  }
}
