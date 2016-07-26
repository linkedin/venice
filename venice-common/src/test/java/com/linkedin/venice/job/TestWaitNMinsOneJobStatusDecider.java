package com.linkedin.venice.job;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
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
  public void testCheckJobStatusWhenJobFail() {
    OfflineJob job = new OfflineJob(1, topic, numberOfPartition, replicationFactor);
    job.setStatus(ExecutionStatus.STARTED);
    job.updateExecutingTasks(partitionAssignment);
    createTasksAndUpdateJob(job, numberOfPartition,replicationFactor,ExecutionStatus.STARTED, -1,-1);
    createTasksAndUpdateJob(job, numberOfPartition,1,ExecutionStatus.ERROR,-1,-1);
    Assert.assertEquals(ExecutionStatus.STARTED, waitNMinsOneDecider.checkJobStatus(job),
        "Only one task is failed for each of partition. Job should be running");

    Task t = new Task(job.generateTaskId(0, nodeId + "1"), 0, nodeId + "1", ExecutionStatus.STARTED);
    t.setStatus(ExecutionStatus.ERROR);
    job.updateTaskStatus(t);

    Assert.assertEquals(ExecutionStatus.ERROR, waitNMinsOneDecider.checkJobStatus(job),
        "In partition 0, 2 tasks are failed, job should be failed.");
  }

  @Test
  public void testHasEnoughTaskExecutor() {
    OfflineJob job = new OfflineJob(1, topic, numberOfPartition, replicationFactor);
    Assert.assertTrue(waitNMinsOneDecider.hasEnoughTaskExecutors(job, partitionAssignment), "No enough executors");

    partitionAssignment.removePartition(1);
    List<Instance> instances = createInstances(replicationFactor - 1);
    partitionAssignment.addPartition(new Partition(1, instances, instances, Collections.emptyList()));

    Assert.assertTrue(waitNMinsOneDecider.hasEnoughTaskExecutors(job, partitionAssignment),
        "Partition-1 miss one replica, In N-1 strategy, decider should return true.");

    partitionAssignment.removePartition(1);
    instances = createInstances(replicationFactor);
    try {
      partitionAssignment.addPartition(new Partition(numberOfPartition + 1, instances, instances, Collections.emptyList()));
      waitNMinsOneDecider.hasEnoughTaskExecutors(job, partitionAssignment);
      Assert.fail("Invalid partition id, decider should throw an exception.");
    } catch (VeniceException e) {
      //expected
    }

    partitionAssignment.removePartition(numberOfPartition + 1);
    Assert.assertFalse(waitNMinsOneDecider.hasEnoughTaskExecutors(job, partitionAssignment),
        "Partition number is smaller than required, decider should return false.");
    partitionAssignment = new PartitionAssignment(topic , numberOfPartition);
    Assert.assertFalse(waitNMinsOneDecider.hasEnoughTaskExecutors(job, partitionAssignment),
        "Partition number is smaller than required, decider should return false.");
  }
}
