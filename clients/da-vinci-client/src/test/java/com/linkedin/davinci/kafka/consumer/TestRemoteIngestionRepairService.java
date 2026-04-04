package com.linkedin.davinci.kafka.consumer;

import com.linkedin.venice.exceptions.VeniceException;
import java.util.concurrent.LinkedBlockingDeque;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestRemoteIngestionRepairService {
  @Test
  public void testIngestionRepairService() throws Exception {
    // Do not start the service to avoid the background thread racing with pollRepairTasks() calls below.
    // Instead, register tasks directly into the internal map.
    RemoteIngestionRepairService repairService = new RemoteIngestionRepairService(1000000);
    StoreIngestionTask mockBrokenIngestionTask = Mockito.mock(StoreIngestionTask.class);
    StoreIngestionTask mockWorkingIngestionTask = Mockito.mock(StoreIngestionTask.class);

    Runnable brokenTask = () -> {
      throw new VeniceException("AAAAHHH!!!!");
    };
    Runnable workingTask1 = () -> {};
    Runnable workingTask2 = () -> {/* This task works and should clear */};

    repairService.getIngestionRepairTasks()
        .computeIfAbsent(mockBrokenIngestionTask, k -> new LinkedBlockingDeque<>())
        .offer(brokenTask);
    repairService.getIngestionRepairTasks()
        .computeIfAbsent(mockWorkingIngestionTask, k -> new LinkedBlockingDeque<>())
        .offer(workingTask1);
    repairService.getIngestionRepairTasks()
        .computeIfAbsent(mockBrokenIngestionTask, k -> new LinkedBlockingDeque<>())
        .offer(workingTask2);

    // We should expect two entries in our map
    Assert.assertEquals(repairService.getIngestionRepairTasks().size(), 2);
    Assert.assertEquals(repairService.getIngestionRepairTasks().get(mockBrokenIngestionTask).size(), 2);
    Assert.assertEquals(repairService.getIngestionRepairTasks().get(mockWorkingIngestionTask).size(), 1);

    // Poll tasks
    repairService.pollRepairTasks();

    // One should complete
    // We should expect two entries in our map
    Assert.assertEquals(repairService.getIngestionRepairTasks().size(), 2);
    Assert.assertEquals(repairService.getIngestionRepairTasks().get(mockBrokenIngestionTask).size(), 2);
    Assert.assertEquals(repairService.getIngestionRepairTasks().get(mockWorkingIngestionTask).size(), 0);

    // Poll Tasks
    repairService.pollRepairTasks();

    // Another should complete
    Assert.assertEquals(repairService.getIngestionRepairTasks().size(), 2);
    Assert.assertEquals(repairService.getIngestionRepairTasks().get(mockBrokenIngestionTask).size(), 1);
    Assert.assertEquals(repairService.getIngestionRepairTasks().get(mockWorkingIngestionTask).size(), 0);

    // Unregister
    repairService.unregisterRepairTasksForStoreIngestionTask(mockBrokenIngestionTask);
    Assert.assertEquals(repairService.getIngestionRepairTasks().size(), 1);
    Assert.assertNull(repairService.getIngestionRepairTasks().get(mockBrokenIngestionTask));

    // Unregister again!
    repairService.unregisterRepairTasksForStoreIngestionTask(mockWorkingIngestionTask);
    Assert.assertEquals(repairService.getIngestionRepairTasks().size(), 0);
  }
}
