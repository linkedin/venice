package com.linkedin.davinci.stats;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import io.tehuti.metrics.MetricsRepository;
import java.time.Clock;
import org.testng.Assert;
import org.testng.annotations.Test;


public class NativeMetadataRepositoryStatsTest {
  @Test
  public void testStats() {
    Clock mockClock = mock(Clock.class);
    doReturn(1000L).when(mockClock).millis();
    String store1 = "testStore1";
    String store2 = "testStore2";
    NativeMetadataRepositoryStats stats = new NativeMetadataRepositoryStats(new MetricsRepository(), "test", mockClock);
    Assert.assertEquals(stats.getMetadataStalenessHighWatermarkMs(), Double.NaN);
    stats.updateCacheTimestamp(store1, 0);
    Assert.assertEquals(stats.getMetadataStalenessHighWatermarkMs(), 1000d);
    stats.updateCacheTimestamp(store2, 1000);
    Assert.assertEquals(stats.getMetadataStalenessHighWatermarkMs(), 1000d);
    doReturn(1500L).when(mockClock).millis();
    stats.updateCacheTimestamp(store1, 1100);
    Assert.assertEquals(stats.getMetadataStalenessHighWatermarkMs(), 500d);
    stats.removeCacheTimestamp(store2);
    Assert.assertEquals(stats.getMetadataStalenessHighWatermarkMs(), 400d);
    stats.removeCacheTimestamp(store1);
    Assert.assertEquals(stats.getMetadataStalenessHighWatermarkMs(), Double.NaN);
  }
}
