package com.linkedin.venice.job;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixState;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestWaitNMinsOneJobStatusDecider extends TestJobStatusDecider {
  private WaitNMinusOneJobStatusDeicder waitNMinsOneDecider = new WaitNMinusOneJobStatusDeicder();

  @BeforeMethod
  public void setup() {
    partitionAssignment = new PartitionAssignment(topic, numberOfPartition);
    createPartitions(numberOfPartition, replicationFactor);
  }

  @Test
  public void testCheckJobStatusNotRunning() {
    OfflineJob job = new OfflineJob(1, topic, numberOfPartition, replicationFactor);
    try {
      waitNMinsOneDecider.checkJobStatus(job);
      Assert.fail("Job is not running.");
    } catch (VeniceException e) {
      //expected
    }
  }

  @Test
  public void testCheckJobStatus() {
    OfflineJob job = new OfflineJob(1, topic, numberOfPartition, replicationFactor);
    job.setStatus(ExecutionStatus.STARTED);
    job.updateExecutingTasks(partitionAssignment);
    Assert.assertEquals(waitNMinsOneDecider.checkJobStatus(job), ExecutionStatus.STARTED,
        "Did not get any updates. SHould still be in running status.");
    createTasksAndUpdateJob(job, numberOfPartition, replicationFactor, ExecutionStatus.STARTED, -1, -1);
    Assert.assertEquals(ExecutionStatus.STARTED, waitNMinsOneDecider.checkJobStatus(job),
        "All of tasks are just started, should be in running status.");
    createTasksAndUpdateJob(job, numberOfPartition, replicationFactor, ExecutionStatus.COMPLETED, 1, 2);
    Task t = new Task(job.generateTaskId(1, nodeId + "2"), 1, nodeId + "2", ExecutionStatus.PROGRESS);
    job.updateTaskStatus(t);
    //Only one task is not terminated
    Assert.assertEquals(ExecutionStatus.COMPLETED, waitNMinsOneDecider.checkJobStatus(job),
        "There is only one task in progress status, in N-1 strategy, job should be terminated.");

    t.setStatus(ExecutionStatus.ERROR);
    job.updateTaskStatus(t);
    //Only one task is not terminated
    Assert.assertEquals(ExecutionStatus.COMPLETED, waitNMinsOneDecider.checkJobStatus(job),
        "Only one task si failed, in N-1 strategy, job should be completed.");
  }

  @Test
  public void testHasEnoughTaskExecutor() {
    OfflineJob job = new OfflineJob(1, topic, numberOfPartition, replicationFactor);
    Assert.assertTrue(waitNMinsOneDecider.hasEnoughTaskExecutorsToStart(job, partitionAssignment), "No enough executors");

    partitionAssignment.removePartition(1);
    List<Instance> instances = createInstances(replicationFactor - 1);
    Map<String, List<Instance>> stateToInstancesMap = new HashMap<>();
    stateToInstancesMap.put(HelixState.ONLINE_STATE, instances);
    stateToInstancesMap.put(HelixState.BOOTSTRAP_STATE, instances);
    partitionAssignment.addPartition(new Partition(1, stateToInstancesMap));

    Assert.assertTrue(waitNMinsOneDecider.hasEnoughTaskExecutorsToStart(job, partitionAssignment),
        "Partition-1 miss one replica, In N-1 strategy, decider should return true.");

    partitionAssignment.removePartition(1);
    instances = createInstances(replicationFactor);
    stateToInstancesMap = new HashMap<>();
    stateToInstancesMap.put(HelixState.ONLINE_STATE, instances);
    stateToInstancesMap.put(HelixState.BOOTSTRAP_STATE, instances);
    try {
      partitionAssignment.addPartition(new Partition(numberOfPartition + 1, stateToInstancesMap));
      waitNMinsOneDecider.hasEnoughTaskExecutorsToStart(job, partitionAssignment);
      Assert.fail("Invalid partition id, decider should throw an exception.");
    } catch (VeniceException e) {
      //expected
    }

    partitionAssignment.removePartition(numberOfPartition + 1);
    Assert.assertFalse(waitNMinsOneDecider.hasEnoughTaskExecutorsToStart(job, partitionAssignment),
        "Partition number is smaller than required, decider should return false.");
    partitionAssignment = new PartitionAssignment(topic , numberOfPartition);
    Assert.assertFalse(waitNMinsOneDecider.hasEnoughTaskExecutorsToStart(job, partitionAssignment),
        "Partition number is smaller than required, decider should return false.");
  }

  @Test
  public void testHasMinimumTaskExecutorsToKeepRunning() {
    OfflineJob job = new OfflineJob(1, topic, numberOfPartition, replicationFactor);
    partitionAssignment.removePartition(1);
    List<Instance> errorInstances = createInstances(1);
    List<Instance> readyInstances = createInstances(replicationFactor - 2);
    Map<String, List<Instance>> stateToInstancesMap = new HashMap<>();
    stateToInstancesMap.put(HelixState.ONLINE_STATE, readyInstances);
    stateToInstancesMap.put(HelixState.ERROR_STATE, errorInstances);
    partitionAssignment.addPartition(new Partition(1, stateToInstancesMap));

    Assert.assertTrue(waitNMinsOneDecider.hasMinimumTaskExecutorsToKeepRunning(job, partitionAssignment));

    partitionAssignment.removePartition(1);
    errorInstances = createInstances(2);
    readyInstances = createInstances(replicationFactor - 2);
    stateToInstancesMap = new HashMap<>();
    stateToInstancesMap.put(HelixState.ONLINE_STATE, readyInstances);
    stateToInstancesMap.put(HelixState.ERROR_STATE, errorInstances);
    partitionAssignment.addPartition(new Partition(1, stateToInstancesMap));

    Assert.assertFalse(waitNMinsOneDecider.hasMinimumTaskExecutorsToKeepRunning(job, partitionAssignment));

    partitionAssignment.removePartition(1);
    stateToInstancesMap = new HashMap<>();
    partitionAssignment.addPartition(new Partition(1, stateToInstancesMap));

    Assert.assertFalse(waitNMinsOneDecider.hasMinimumTaskExecutorsToKeepRunning(job, partitionAssignment));
  }
}
