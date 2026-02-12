package com.linkedin.venice.router.api.routing.helix;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.linkedin.alpini.base.concurrency.TimeoutProcessor;
import com.linkedin.venice.helix.HelixInstanceConfigRepository;
import io.tehuti.metrics.MetricsRepository;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class HelixGroupSelectorTest {
  private MetricsRepository metricsRepository;
  private HelixInstanceConfigRepository instanceConfigRepository;
  private TimeoutProcessor timeoutProcessor;

  @BeforeMethod
  public void setUp() {
    metricsRepository = new MetricsRepository();
    instanceConfigRepository = mock(HelixInstanceConfigRepository.class);
    timeoutProcessor = mock(TimeoutProcessor.class);

    // Mock timeout processor to return a mock future
    doReturn(mock(TimeoutProcessor.TimeoutFuture.class)).when(timeoutProcessor).schedule(any(), anyLong(), any());
  }

  @AfterMethod
  public void tearDown() {
    if (metricsRepository != null) {
      metricsRepository.close();
    }
  }

  @Test
  public void testConstructorWithRoundRobinStrategy() {
    doReturn(3).when(instanceConfigRepository).getGroupCount();

    HelixGroupSelector selector = new HelixGroupSelector(
        metricsRepository,
        instanceConfigRepository,
        HelixGroupSelectionStrategyEnum.ROUND_ROBIN,
        timeoutProcessor);

    // RoundRobin uses simple modulo, so requestId 0 with groupCount 3 should return 0
    int group = selector.selectGroup(0L, 3);
    Assert.assertTrue(group >= 0 && group < 3);
  }

  @Test
  public void testConstructorWithLeastLoadedStrategy() {
    doReturn(3).when(instanceConfigRepository).getGroupCount();

    HelixGroupSelector selector = new HelixGroupSelector(
        metricsRepository,
        instanceConfigRepository,
        HelixGroupSelectionStrategyEnum.LEAST_LOADED,
        timeoutProcessor);

    // Should create successfully with LeastLoaded strategy
    int group = selector.selectGroup(1L, 3);
    Assert.assertTrue(group >= 0 && group < 3);
  }

  @Test
  public void testSelectGroupReturnsValidGroup() {
    doReturn(5).when(instanceConfigRepository).getGroupCount();

    HelixGroupSelector selector = new HelixGroupSelector(
        metricsRepository,
        instanceConfigRepository,
        HelixGroupSelectionStrategyEnum.ROUND_ROBIN,
        timeoutProcessor);

    // Test multiple selections
    for (int i = 0; i < 10; i++) {
      int group = selector.selectGroup(i, 5);
      Assert.assertTrue(group >= 0 && group < 5, "Group should be in range [0, 5)");
    }
  }

  @Test
  public void testFinishRequestDelegates() {
    HelixGroupSelector selector = new HelixGroupSelector(
        metricsRepository,
        instanceConfigRepository,
        HelixGroupSelectionStrategyEnum.ROUND_ROBIN,
        timeoutProcessor);

    // RoundRobin's finishRequest is a no-op, so this should just not throw
    selector.finishRequest(1L, 0, 100.0);
  }

  @Test
  public void testGetInstanceGroupIdDelegates() {
    doReturn(2).when(instanceConfigRepository).getInstanceGroupId("instance1");

    HelixGroupSelector selector = new HelixGroupSelector(
        metricsRepository,
        instanceConfigRepository,
        HelixGroupSelectionStrategyEnum.ROUND_ROBIN,
        timeoutProcessor);

    int groupId = selector.getInstanceGroupId("instance1");

    Assert.assertEquals(groupId, 2);
    verify(instanceConfigRepository, times(1)).getInstanceGroupId("instance1");
  }

  @Test
  public void testGetGroupCountDelegates() {
    doReturn(4).when(instanceConfigRepository).getGroupCount();

    HelixGroupSelector selector = new HelixGroupSelector(
        metricsRepository,
        instanceConfigRepository,
        HelixGroupSelectionStrategyEnum.ROUND_ROBIN,
        timeoutProcessor);

    int groupCount = selector.getGroupCount();

    Assert.assertEquals(groupCount, 4);
    verify(instanceConfigRepository, times(1)).getGroupCount();
  }

  @Test
  public void testLeastLoadedStrategyWithFinishRequest() {
    doReturn(3).when(instanceConfigRepository).getGroupCount();

    HelixGroupSelector selector = new HelixGroupSelector(
        metricsRepository,
        instanceConfigRepository,
        HelixGroupSelectionStrategyEnum.LEAST_LOADED,
        timeoutProcessor);

    // Select a group
    long requestId = 12345L;
    int group = selector.selectGroup(requestId, 3);

    // Finish the request - LeastLoaded tracks pending requests
    selector.finishRequest(requestId, group, 50.0);

    // Should not throw, and internally should decrement counter
  }

  @Test
  public void testRoundRobinDistributesEvenly() {
    HelixGroupSelector selector = new HelixGroupSelector(
        metricsRepository,
        instanceConfigRepository,
        HelixGroupSelectionStrategyEnum.ROUND_ROBIN,
        timeoutProcessor);

    int groupCount = 3;
    int[] groupCounts = new int[groupCount];

    // Make 300 selections with sequential request IDs
    for (int i = 0; i < 300; i++) {
      int group = selector.selectGroup(i, groupCount);
      groupCounts[group]++;
    }

    // Each group should get exactly 100 requests with round-robin
    for (int i = 0; i < groupCount; i++) {
      Assert.assertEquals(groupCounts[i], 100, "Group " + i + " should have exactly 100 requests");
    }
  }

  @Test
  public void testSelectGroupWithSingleGroup() {
    HelixGroupSelector selector = new HelixGroupSelector(
        metricsRepository,
        instanceConfigRepository,
        HelixGroupSelectionStrategyEnum.ROUND_ROBIN,
        timeoutProcessor);

    // With only 1 group, all requests should go to group 0
    for (int i = 0; i < 10; i++) {
      int group = selector.selectGroup(i, 1);
      Assert.assertEquals(group, 0, "With single group, should always return 0");
    }
  }
}
