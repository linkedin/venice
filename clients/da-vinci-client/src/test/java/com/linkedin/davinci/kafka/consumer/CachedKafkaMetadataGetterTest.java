package com.linkedin.davinci.kafka.consumer;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.TopicDoesNotExistException;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Test;


public class CachedKafkaMetadataGetterTest {
  @Test
  public void testCacheWillResetStatusWhenExceptionIsThrown() {
    CachedKafkaMetadataGetter cachedKafkaMetadataGetter = new CachedKafkaMetadataGetter(1000);
    CachedKafkaMetadataGetter.KafkaMetadataCacheKey key =
        new CachedKafkaMetadataGetter.KafkaMetadataCacheKey("server", "topic", 1);
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

    // TopicDoesNotExistException is caught and thrown.
    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      Assert.assertThrows(
          TopicDoesNotExistException.class,
          () -> cachedKafkaMetadataGetter.fetchMetadata(key, offsetCache, () -> {
            throw new TopicDoesNotExistException("dummy exception");
          }));
    });

    // TopicDoesNotExistException flag is cleaned up and other types of exception won't be thrown.
    long initialExpiredTime = offsetCache.get(key).getExpiryTimeNs();
    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      Long actualResult = cachedKafkaMetadataGetter.fetchMetadata(key, offsetCache, () -> {
        throw new VeniceException("do not throw this exception!");
      });
      Long expectedResult = 2L;
      Assert.assertEquals(expectedResult, actualResult);
    });
    // Value is not updated at all.
    Assert.assertEquals(offsetCache.get(key).getExpiryTimeNs(), initialExpiredTime);

    // Successful call will update the value from 2 to 3.
    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      Long actualResult = cachedKafkaMetadataGetter.fetchMetadata(key, offsetCache, () -> 3L);
      Long expectedResult = 3L;
      Assert.assertEquals(actualResult, expectedResult);
    });

  }
}
