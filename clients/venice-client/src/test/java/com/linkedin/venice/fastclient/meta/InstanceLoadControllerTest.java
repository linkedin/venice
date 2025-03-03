package com.linkedin.venice.fastclient.meta;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.utils.TestUtils;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;


public class InstanceLoadControllerTest {
  @Test
  public void testInstanceLoadController() {
    InstanceHealthMonitorConfig config = mock(InstanceHealthMonitorConfig.class);
    doReturn(true).when(config).isLoadControllerEnabled();
    doReturn(5).when(config).getLoadControllerWindowSizeInSec();
    doReturn(1.5d).when(config).getLoadControllerAcceptMultiplier();
    doReturn(0.9).when(config).getLoadControllerMaxRejectionRatio();
    doReturn(1).when(config).getLoadControllerRejectionRatioUpdateIntervalInSec();

    InstanceLoadController instanceLoadController = new InstanceLoadController(config);
    String instanceId1 = "instance1";
    String instanceId2 = "instance2";

    instanceLoadController.recordResponse(instanceId1, 200);
    instanceLoadController.recordResponse(instanceId1, 500);
    instanceLoadController.recordResponse(instanceId2, 200);
    instanceLoadController.recordResponse(instanceId2, 500);

    assertEquals(instanceLoadController.getTotalNumberOfOverLoadedInstances(), 0);

    for (int i = 0; i < 10; ++i) {
      instanceLoadController.recordResponse(instanceId1, HttpConstants.SC_SERVICE_OVERLOADED);
      instanceLoadController.recordResponse(instanceId2, HttpConstants.SC_SERVICE_OVERLOADED);
    }

    TestUtils.waitForNonDeterministicAssertion(
        10,
        TimeUnit.SECONDS,
        () -> assertEquals(instanceLoadController.getTotalNumberOfOverLoadedInstances(), 2));

    boolean requestRejectedByInstance1 = false;
    boolean requestRejectedByInstance2 = false;
    for (int i = 0; i < 100; ++i) {
      if (instanceLoadController.shouldRejectRequest(instanceId1)) {
        requestRejectedByInstance1 = true;
      }
      if (instanceLoadController.shouldRejectRequest(instanceId2)) {
        requestRejectedByInstance2 = true;
      }
    }
    assertEquals(requestRejectedByInstance1, true);
    assertEquals(requestRejectedByInstance2, true);
  }
}
