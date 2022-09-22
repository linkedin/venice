package com.linkedin.davinci.kafka.consumer;

import com.linkedin.venice.exceptions.VeniceException;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestRemoteIngestionRepairService {
  @Test
  public void testIngestionRepairService() throws Exception {
    RemoteIngestionRepairService repairService = new RemoteIngestionRepairService(1000000);
    repairService.start();
    StoreIngestionTask mockBrockenIngestionTask = Mockito.mock(StoreIngestionTask.class);
    StoreIngestionTask mockWorkingIngestionTask = Mockito.mock(StoreIngestionTask.class);
    repairService.registerRepairTask(mockBrockenIngestionTask, () -> {
      throw new VeniceException("AAAAHHH!!!!");
    });
    repairService.registerRepairTask(mockWorkingIngestionTask, () -> {});
    repairService.registerRepairTask(mockBrockenIngestionTask, () -> {/* This task works and should clear */});
    repairService.stop();

    // We should expect two entries in our map
    Assert.assertEquals(repairService.getIngestionRepairTasks().size(), 2);
    Assert.assertEquals(repairService.getIngestionRepairTasks().get(mockBrockenIngestionTask).size(), 2);
    Assert.assertEquals(repairService.getIngestionRepairTasks().get(mockWorkingIngestionTask).size(), 1);

    // Poll tasks
    repairService.pollRepairTasks();

    // One should complete
    // We should expect two entries in our map
    Assert.assertEquals(repairService.getIngestionRepairTasks().size(), 2);
    Assert.assertEquals(repairService.getIngestionRepairTasks().get(mockBrockenIngestionTask).size(), 2);
    Assert.assertEquals(repairService.getIngestionRepairTasks().get(mockWorkingIngestionTask).size(), 0);

    // Poll Tasks
    repairService.pollRepairTasks();

    // Another should complete
    Assert.assertEquals(repairService.getIngestionRepairTasks().size(), 2);
    Assert.assertEquals(repairService.getIngestionRepairTasks().get(mockBrockenIngestionTask).size(), 1);
    Assert.assertEquals(repairService.getIngestionRepairTasks().get(mockWorkingIngestionTask).size(), 0);

    // Unregister
    repairService.unregisterRepairTasksForStoreIngestionTask(mockBrockenIngestionTask);
    Assert.assertEquals(repairService.getIngestionRepairTasks().size(), 1);
    Assert.assertNull(repairService.getIngestionRepairTasks().get(mockBrockenIngestionTask));

    // Unregister again!
    repairService.unregisterRepairTasksForStoreIngestionTask(mockWorkingIngestionTask);
    Assert.assertEquals(repairService.getIngestionRepairTasks().size(), 0);
  }
}
