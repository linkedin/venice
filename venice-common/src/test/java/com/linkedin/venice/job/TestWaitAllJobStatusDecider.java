package com.linkedin.venice.job;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Partition;
import java.util.HashMap;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestWaitAllJobStatusDecider extends TestJobStatusDecider {
  private WaitAllJobStatsDecider waitAllDecider = new WaitAllJobStatsDecider();

  @BeforeMethod
  public void setup() {
    createPartitions(numberOfPartition, replicationFactor);
  }

  @AfterMethod
  public void cleanup() {
    partitions.clear();
  }

  @Test
  public void testCheckJobStatusNotRunning() {
    OfflineJob job = new OfflineJob(1, topic, numberOfPartition, replicationFactor);
    try {
      waitAllDecider.checkJobStatus(job);
      Assert.fail("Job is not running.");
    } catch (VeniceException e) {
      //expected
    }
  }

  @Test
  public void testCheckJobStatus() {
    OfflineJob job = new OfflineJob(1, topic, numberOfPartition, replicationFactor);
    job.setStatus(ExecutionStatus.STARTED);
    job.updateExecutingTasks(partitions);
    Assert.assertEquals(waitAllDecider.checkJobStatus(job), ExecutionStatus.STARTED,
        "Did not get any updates. SHould still be in running status.");

    createTasksAndUpdateJob(job, numberOfPartition, replicationFactor, ExecutionStatus.STARTED, -1, -1);
    Assert.assertEquals(ExecutionStatus.STARTED, waitAllDecider.checkJobStatus(job),
        "All of tasks are just started, should be in runing status.");
    createTasksAndUpdateJob(job, numberOfPartition, replicationFactor, ExecutionStatus.COMPLETED, 1, 2);
    Task t = new Task(job.generateTaskId(1, nodeId + "2"), 1, nodeId + "2", ExecutionStatus.PROGRESS);
    job.updateTaskStatus(t);
    //Only one task is not terminated
    Assert.assertEquals(ExecutionStatus.STARTED, waitAllDecider.checkJobStatus(job),
        "There is still one task in progress status, job is still running.");

    t.setStatus(ExecutionStatus.COMPLETED);
    job.updateTaskStatus(t);
    //Only one task is not terminated
    Assert.assertEquals(ExecutionStatus.COMPLETED, waitAllDecider.checkJobStatus(job),
        "All the tasks are completed, job is completed.");
  }

  @Test
  public void testCheckJobStatusWhenJobFail() {
    OfflineJob job = new OfflineJob(1, topic, numberOfPartition, replicationFactor);
    job.setStatus(ExecutionStatus.STARTED);
    job.updateExecutingTasks(partitions);
    createTasksAndUpdateJob(job,numberOfPartition,replicationFactor,ExecutionStatus.STARTED,-1,-1);
    createTasksAndUpdateJob(job, numberOfPartition,replicationFactor,ExecutionStatus.COMPLETED, 2, 0);

    Task t = new Task(job.generateTaskId(2, nodeId + "0"), 2, nodeId + "0", ExecutionStatus.PROGRESS);
    job.updateTaskStatus(t);
    //Only one task is not terminated
    Assert.assertEquals(ExecutionStatus.STARTED, waitAllDecider.checkJobStatus(job),
        "There is still one task in progress status, job is still running.");
    t.setStatus(ExecutionStatus.ERROR);
    job.updateTaskStatus(t);
    //Only one task is not terminated
    Assert.assertEquals(ExecutionStatus.ERROR, waitAllDecider.checkJobStatus(job),
        "One task is failed, based on current strategy, job should be failed.");
  }

  @Test
  public void testHasEnoughTaskExecutor() {
    OfflineJob job = new OfflineJob(1, topic, numberOfPartition, replicationFactor);
    Assert.assertTrue(waitAllDecider.hasEnoughTaskExecutors(job, partitions.values()), "No enough executors");

    partitions.remove(1);
    partitions.put(1, new Partition(1, topic, createInstances(replicationFactor - 1)));

    Assert.assertFalse(waitAllDecider.hasEnoughTaskExecutors(job, partitions.values()),
        "Partition-1 miss one replica, decider should return false to indicate no enough task executors.");

    partitions.remove(1);
    partitions.put(numberOfPartition + 1,
        new Partition(numberOfPartition + 1, topic, createInstances(replicationFactor)));

    try {
      waitAllDecider.hasEnoughTaskExecutors(job, partitions.values());
      Assert.fail("Invalid partition id, decider should throw an exception.");
    } catch (VeniceException e) {
      //expected
    }

    partitions.remove(numberOfPartition + 1);
    Assert.assertFalse(waitAllDecider.hasEnoughTaskExecutors(job, partitions.values()),
        "Partition number is smaller than required, decider should return false.");
    partitions = new HashMap<>();
    waitAllDecider.hasEnoughTaskExecutors(job, partitions.values());
    Assert.assertFalse(waitAllDecider.hasEnoughTaskExecutors(job, partitions.values()),
        "Partition number is smaller than required, decider should return false.");
  }
}
