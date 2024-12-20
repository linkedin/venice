package com.linkedin.venice.router.api.routing.helix;

import org.testng.Assert;
import org.testng.annotations.Test;


public class TestHelixGroupRoundRobinStrategy {
  @Test
  public void testSelectGroup() {
    HelixGroupRoundRobinStrategy strategy = new HelixGroupRoundRobinStrategy();
    int groupNum = 3;
    Assert.assertEquals(strategy.selectGroup(0, groupNum), 0);
    strategy.finishRequest(0, 0, 1);
    Assert.assertEquals(strategy.selectGroup(1, groupNum), 1);
    strategy.finishRequest(1, 1, 1);
    Assert.assertEquals(strategy.selectGroup(2, groupNum), 2);
    strategy.finishRequest(2, 2, 1);
    Assert.assertEquals(strategy.selectGroup(3, groupNum), 0);
    strategy.finishRequest(3, 0, 1);
    Assert.assertEquals(strategy.selectGroup(4, groupNum), 1);
    strategy.finishRequest(4, 1, 1);
    Assert.assertEquals(strategy.selectGroup(5, groupNum), 2);
    strategy.finishRequest(5, 2, 1);
  }
}
