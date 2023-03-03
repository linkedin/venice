package com.linkedin.davinci.kafka.consumer;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.testng.Assert;
import org.testng.annotations.Test;


public class CachedKafkaMetadataGetterTest {
  private PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();

  @Test
  public void testCacheWillResetStatusWhenExceptionIsThrown() {
    CachedKafkaMetadataGetter cachedKafkaMetadataGetter = new CachedKafkaMetadataGetter(1000);
    CachedKafkaMetadataGetter.KafkaMetadataCacheKey key = new CachedKafkaMetadataGetter.KafkaMetadataCacheKey(
        "server",
        new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic(Utils.getUniqueTopicString("topic")), 1));
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
