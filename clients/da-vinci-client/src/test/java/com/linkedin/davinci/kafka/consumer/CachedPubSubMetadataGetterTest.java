package com.linkedin.davinci.kafka.consumer;

import static org.mockito.ArgumentMatchers.*;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.testng.Assert;
import org.testng.annotations.Test;


public class CachedPubSubMetadataGetterTest {
  private final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();

  @Test
  public void testCacheWillResetStatusWhenExceptionIsThrown() {
    CachedPubSubMetadataGetter cachedPubSubMetadataGetter = new CachedPubSubMetadataGetter(1000);
    CachedPubSubMetadataGetter.PubSubMetadataCacheKey key = new CachedPubSubMetadataGetter.PubSubMetadataCacheKey(
        "server",
        new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic(TestUtils.getUniqueTopicString("topic")), 1));
    Map<CachedPubSubMetadataGetter.PubSubMetadataCacheKey, CachedPubSubMetadataGetter.ValueAndExpiryTime<Long>> offsetCache =
        new VeniceConcurrentHashMap<>();
    CachedPubSubMetadataGetter.ValueAndExpiryTime<Long> valueCache =
        new CachedPubSubMetadataGetter.ValueAndExpiryTime<>(1L, System.nanoTime());
    offsetCache.put(key, valueCache);
    // Successful call will update the value from 1 to 2.
    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      Long actualResult = cachedPubSubMetadataGetter.fetchMetadata(key, offsetCache, () -> 2L);
      Long expectedResult = 2L;
      Assert.assertEquals(actualResult, expectedResult);
    });

    // For persisting exception, it will be caught and thrown.
    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      Assert
          .assertThrows(VeniceException.class, () -> cachedPubSubMetadataGetter.fetchMetadata(key, offsetCache, () -> {
            throw new VeniceException("dummy exception");
          }));
    });

    // Reset the cached value to 1.
    valueCache = new CachedPubSubMetadataGetter.ValueAndExpiryTime<>(1L, System.nanoTime());
    offsetCache.put(key, valueCache);

    // The first call throws a transient exception, and it should be updated to expected value after second call.
    AtomicBoolean exceptionFlag = new AtomicBoolean(false);
    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, false, true, () -> {
      Long actualResult = cachedPubSubMetadataGetter.fetchMetadata(key, offsetCache, () -> {
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
