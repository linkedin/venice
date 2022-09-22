package com.linkedin.alpini.base.monitoring;

import com.linkedin.alpini.base.statistics.LongStats;
import org.testng.Assert;
import org.testng.annotations.Test;


@Test(groups = "unit")
public class TestNullCallTracker {
  private final CallTracker _callTracker = CallTracker.nullTracker();

  @Test
  public void invokeEveryMethod() {

    CallCompletion completion = _callTracker.startCall();

    _callTracker.reset();

    _callTracker.trackCall(1L);

    _callTracker.trackCallWithError(1L);
    _callTracker.trackCallWithError(1L, new NullPointerException());

    Assert.assertEquals(_callTracker.getCurrentStartCountTotal(), 0);
    Assert.assertEquals(_callTracker.getCurrentCallCountTotal(), 0);
    Assert.assertEquals(_callTracker.getCurrentErrorCountTotal(), 0);
    Assert.assertEquals(_callTracker.getCurrentConcurrency(), 0);
    Assert.assertEquals(_callTracker.getAverageConcurrency(), new double[0]);
    Assert.assertEquals(_callTracker.getMaxConcurrency(), new int[0]);
    Assert.assertEquals(_callTracker.getStartFrequency(), new int[0]);
    Assert.assertEquals(_callTracker.getStartCount(), new long[0]);
    Assert.assertEquals(_callTracker.getErrorFrequency(), new int[0]);
    Assert.assertEquals(_callTracker.getErrorCount(), new long[0]);

    CallTracker.CallStats stats = _callTracker.getCallStats();

    Assert.assertEquals(_callTracker.getLastResetTime(), 0);
    Assert.assertEquals(_callTracker.getTimeSinceLastStartCall(), 0);

    completion.close();
    completion.closeWithError();
    completion.closeWithError(new NullPointerException());
    completion.closeCompletion(null, new NullPointerException());
    completion.closeCompletion(true, null);

    CallCompletion.combine(completion).close();
    CallCompletion.combine(completion).closeWithError();

    Assert.assertEquals(stats.getCallCountTotal(), 0);
    Assert.assertEquals(stats.getCallStartCountTotal(), 0);
    Assert.assertEquals(stats.getErrorCountTotal(), 0);
    Assert.assertEquals(stats.getConcurrency(), 0);
    Assert.assertEquals(stats.getAverageConcurrency1min(), 0.0);
    Assert.assertEquals(stats.getAverageConcurrency5min(), 0.0);
    Assert.assertEquals(stats.getAverageConcurrency15min(), 0.0);
    Assert.assertEquals(stats.getMaxConcurrency1min(), 0);
    Assert.assertEquals(stats.getMaxConcurrency5min(), 0);
    Assert.assertEquals(stats.getMaxConcurrency15min(), 0);
    Assert.assertEquals(stats.getStartFrequency1min(), 0);
    Assert.assertEquals(stats.getStartFrequency5min(), 0);
    Assert.assertEquals(stats.getStartFrequency15min(), 0);
    Assert.assertEquals(stats.getErrorFrequency1min(), 0);
    Assert.assertEquals(stats.getErrorFrequency5min(), 0);
    Assert.assertEquals(stats.getErrorFrequency15min(), 0);
    Assert.assertEquals(stats.getOutstandingStartTimeAvg(), 0);
    Assert.assertEquals(stats.getOutstandingCount(), 0);

    LongStats longStats = stats.getCallTimeStats();

    Assert.assertEquals(longStats.getLongCount(), 0);
    Assert.assertEquals(longStats.getAverage(), 0.0);
    Assert.assertEquals(longStats.getStandardDeviation(), 0.0);
    Assert.assertNull(longStats.getMinimum());
    Assert.assertNull(longStats.getMaximum());
    Assert.assertNull(longStats.get50Pct());
    Assert.assertNull(longStats.get90Pct());
    Assert.assertNull(longStats.get95Pct());
    Assert.assertNull(longStats.get99Pct());
    Assert.assertNull(longStats.get99_9Pct());

  }

}
