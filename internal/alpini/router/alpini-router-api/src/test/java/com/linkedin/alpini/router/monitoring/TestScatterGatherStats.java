package com.linkedin.alpini.router.monitoring;

import org.testng.Assert;
import org.testng.annotations.Test;


public class TestScatterGatherStats {
  @Test(groups = "unit")
  public void testStats() {
    ScatterGatherStats stats = new ScatterGatherStats();
    ScatterGatherStats.Delta delta = stats.new Delta();
    delta.incrementTotalRetries();
    delta.incrementTotalRetriedKeys(100);
    delta.incrementTotalRetriesDiscarded();
    delta.incrementTotalRetriesError();
    delta.incrementTotalRetriesWinner();

    Assert.assertEquals(stats.getTotalRetries(), 0);
    Assert.assertEquals(stats.getTotalRetriedKeys(), 0);
    Assert.assertEquals(stats.getTotalRetriesDiscarded(), 0);
    Assert.assertEquals(stats.getTotalRetriesError(), 0);
    Assert.assertEquals(stats.getTotalRetriesWinner(), 0);

    delta.apply();

    Assert.assertEquals(stats.getTotalRetries(), 1);
    Assert.assertEquals(stats.getTotalRetriedKeys(), 100);
    Assert.assertEquals(stats.getTotalRetriesDiscarded(), 1);
    Assert.assertEquals(stats.getTotalRetriesError(), 1);
    Assert.assertEquals(stats.getTotalRetriesWinner(), 1);

    delta.apply();

    Assert.assertEquals(stats.getTotalRetries(), 1);
    Assert.assertEquals(stats.getTotalRetriedKeys(), 100);
    Assert.assertEquals(stats.getTotalRetriesDiscarded(), 1);
    Assert.assertEquals(stats.getTotalRetriesError(), 1);
    Assert.assertEquals(stats.getTotalRetriesWinner(), 1);
  }
}
