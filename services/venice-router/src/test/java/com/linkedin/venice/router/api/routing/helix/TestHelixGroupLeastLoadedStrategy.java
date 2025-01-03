package com.linkedin.venice.router.api.routing.helix;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.alpini.base.concurrency.TimeoutProcessor;
import com.linkedin.venice.router.stats.HelixGroupStats;
import io.tehuti.metrics.MetricsRepository;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestHelixGroupLeastLoadedStrategy {
  @Test
  public void testSelectGroup() {
    TimeoutProcessor timeoutProcessor = mock(TimeoutProcessor.class);
    doReturn(mock(TimeoutProcessor.TimeoutFuture.class)).when(timeoutProcessor).schedule(any(), anyLong(), any());
    HelixGroupLeastLoadedStrategy strategy =
        new HelixGroupLeastLoadedStrategy(timeoutProcessor, 10000, mock(HelixGroupStats.class));
    int groupNum = 3;
    // Group 0 is slow.
    Assert.assertEquals(strategy.selectGroup(0, groupNum), 0);
    Assert.assertEquals(strategy.selectGroup(1, groupNum), 1);
    Assert.assertEquals(strategy.selectGroup(2, groupNum), 2);
    strategy.finishRequest(1, 1, 1);
    strategy.finishRequest(2, 2, 1);
    Assert.assertEquals(strategy.selectGroup(3, groupNum), 1);
    Assert.assertEquals(strategy.selectGroup(4, groupNum), 2);
    strategy.finishRequest(0, 0, 1);
    strategy.finishRequest(3, 1, 1);
    strategy.finishRequest(4, 2, 1);
    // Group 0 is recovered
    Assert.assertEquals(strategy.selectGroup(5, groupNum), 2);
    Assert.assertEquals(strategy.selectGroup(6, groupNum), 0);
  }

  @Test
  public void testLatencyBasedGroupSelection() {
    TimeoutProcessor timeoutProcessor = mock(TimeoutProcessor.class);
    doReturn(mock(TimeoutProcessor.TimeoutFuture.class)).when(timeoutProcessor).schedule(any(), anyLong(), any());
    HelixGroupStats stats = new HelixGroupStats(new MetricsRepository());
    HelixGroupLeastLoadedStrategy strategy = new HelixGroupLeastLoadedStrategy(timeoutProcessor, 10000, stats);
    int groupNum = 3;
    Assert.assertEquals(strategy.selectGroup(0, groupNum), 0);
    Assert.assertEquals(strategy.selectGroup(1, groupNum), 1);
    Assert.assertEquals(strategy.selectGroup(2, groupNum), 2);
    // Group 2 is the fastest one
    strategy.finishRequest(0, 0, 2);
    strategy.finishRequest(1, 1, 3);
    strategy.finishRequest(2, 2, 1);
    Assert.assertEquals(strategy.selectGroup(3, groupNum), 2);
  }
}
