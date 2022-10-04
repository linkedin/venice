package com.linkedin.alpini.router.monitoring;

import org.testng.Assert;
import org.testng.annotations.Test;


public class TestScatterGatherStats {
  @Test(groups = "unit")
  public void testStats() {
    ScatterGatherStats stats = new ScatterGatherStats();
    ScatterGatherStats.Delta delta = stats.new Delta();
    delta.incrementFanoutRequestsSent(3);
    delta.incrementTotalRetries();
    delta.incrementTotalRetriedKeys(100);
    delta.incrementTotalRequestsReceived();
    delta.incrementTotalRetriesDiscarded(1000);
    delta.incrementTotalRetriesError();
    delta.incrementTotalRetriesWinner();
    delta.incrementTotalRetriesOn429();
    delta.incrementTotalRetriesOn503();
    delta.incrementTotalRetriesOn503Winner();
    delta.incrementTotalRetriesOn503Error();

    Assert.assertEquals(stats.getTotalRequestsSent(), 0);
    Assert.assertEquals(stats.getTotalRetries(), 0);
    Assert.assertEquals(stats.getTotalRetriedKeys(), 0);
    Assert.assertEquals(stats.getTotalRequestsReceived(), 0);
    Assert.assertEquals(stats.getTotalRetriesDiscarded(), 0);
    Assert.assertEquals(stats.getTotalDiscardedBytes(), 0);
    Assert.assertEquals(stats.getTotalRetriesError(), 0);
    Assert.assertEquals(stats.getTotalRetriesWinner(), 0);
    Assert.assertEquals(stats.getTotalRetriedOn429(), 0);
    Assert.assertEquals(stats.getTotalRetriesOn503(), 0);
    Assert.assertEquals(stats.getTotalRetriesOn503Winner(), 0);
    Assert.assertEquals(stats.getTotalRetriesOn503Error(), 0);

    delta.apply();

    Assert.assertEquals(stats.getTotalRequestsSent(), 3);
    Assert.assertEquals(stats.getTotalRetries(), 1);
    Assert.assertEquals(stats.getTotalRetriedKeys(), 100);
    Assert.assertEquals(stats.getTotalRequestsReceived(), 1);
    Assert.assertEquals(stats.getTotalRetriesDiscarded(), 1);
    Assert.assertEquals(stats.getTotalDiscardedBytes(), 1000);
    Assert.assertEquals(stats.getTotalRetriesError(), 1);
    Assert.assertEquals(stats.getTotalRetriesWinner(), 1);
    Assert.assertEquals(stats.getTotalRetriedOn429(), 1);
    Assert.assertEquals(stats.getTotalRetriesOn503(), 1);
    Assert.assertEquals(stats.getTotalRetriesOn503Winner(), 1);
    Assert.assertEquals(stats.getTotalRetriesOn503Error(), 1);

    delta.apply();

    Assert.assertEquals(stats.getTotalRequestsSent(), 3);
    Assert.assertEquals(stats.getTotalRetries(), 1);
    Assert.assertEquals(stats.getTotalRetriedKeys(), 100);
    Assert.assertEquals(stats.getTotalRequestsReceived(), 1);
    Assert.assertEquals(stats.getTotalRetriesDiscarded(), 1);
    Assert.assertEquals(stats.getTotalDiscardedBytes(), 1000);
    Assert.assertEquals(stats.getTotalRetriesError(), 1);
    Assert.assertEquals(stats.getTotalRetriesWinner(), 1);
    Assert.assertEquals(stats.getTotalRetriedOn429(), 1);
    Assert.assertEquals(stats.getTotalRetriesOn503(), 1);
    Assert.assertEquals(stats.getTotalRetriesOn503Winner(), 1);
    Assert.assertEquals(stats.getTotalRetriesOn503Error(), 1);
  }
}
