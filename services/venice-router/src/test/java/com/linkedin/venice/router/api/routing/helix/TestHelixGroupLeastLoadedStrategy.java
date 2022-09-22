package com.linkedin.venice.router.api.routing.helix;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.alpini.base.concurrency.TimeoutProcessor;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestHelixGroupLeastLoadedStrategy {
  @Test
  public void testSelectGroup() {
    TimeoutProcessor timeoutProcessor = mock(TimeoutProcessor.class);
    doReturn(mock(TimeoutProcessor.TimeoutFuture.class)).when(timeoutProcessor).schedule(any(), anyLong(), any());
    HelixGroupLeastLoadedStrategy strategy = new HelixGroupLeastLoadedStrategy(timeoutProcessor, 10000);
    int groupNum = 3;
    // Group 0 is slow.
    Assert.assertEquals(strategy.selectGroup(0, groupNum), 0);
    Assert.assertEquals(strategy.selectGroup(1, groupNum), 1);
    Assert.assertEquals(strategy.selectGroup(2, groupNum), 2);
    Assert.assertEquals(strategy.getMaxGroupPendingRequest(), 1);
    Assert.assertEquals(strategy.getMinGroupPendingRequest(), 1);
    Assert.assertEquals(strategy.getAvgGroupPendingRequest(), 1);
    strategy.finishRequest(1, 1);
    strategy.finishRequest(2, 2);
    Assert.assertEquals(strategy.selectGroup(3, groupNum), 1);
    Assert.assertEquals(strategy.selectGroup(4, groupNum), 2);
    strategy.finishRequest(0, 0);
    strategy.finishRequest(3, 1);
    strategy.finishRequest(4, 2);
    // Group 0 is recovered
    Assert.assertEquals(strategy.selectGroup(5, groupNum), 2);
    Assert.assertEquals(strategy.selectGroup(6, groupNum), 0);
  }
}
