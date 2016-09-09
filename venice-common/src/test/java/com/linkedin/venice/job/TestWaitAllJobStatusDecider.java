package com.linkedin.venice.job;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import java.util.Collections;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestWaitAllJobStatusDecider extends TestJobStatusDecider {
  private WaitAllJobStatsDecider waitAllDecider = new WaitAllJobStatsDecider();

  @BeforeMethod
  public void setup() {
    partitionAssignment = new PartitionAssignment(topic, numberOfPartition);
    createPartitions(numberOfPartition, replicationFactor);
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
    job.updateExecutingTasks(partitionAssignment);
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
    job.updateExecutingTasks(partitionAssignment);
    createTasksAndUpdateJob(job,numberOfPartition,replicationFactor,ExecutionStatus.STARTED,-1,-1);

    Task t = new Task(job.generateTaskId(2, nodeId + "0"), 2, nodeId + "0", ExecutionStatus.PROGRESS);
    job.updateTaskStatus(t);
    //Only one task is not terminated
    Assert.assertEquals(ExecutionStatus.STARTED, waitAllDecider.checkJobStatus(job),
        "There is still one task in progress status, job is still running.");
    t.setStatus(ExecutionStatus.ERROR);
    job.updateTaskStatus(t);
    //Only one task is error
    Assert.assertEquals(ExecutionStatus.ERROR, waitAllDecider.checkJobStatus(job),
        "One task is failed, based on current strategy, job should be failed.");
    t = new Task(job.generateTaskId(2, nodeId + "1"), 2, nodeId + "1", ExecutionStatus.ERROR);
    job.updateTaskStatus(t);
    //Two tasks are error
    Assert.assertEquals(ExecutionStatus.ERROR, waitAllDecider.checkJobStatus(job),
        "One task is failed, based on current strategy, job should be failed.");
    // all of tasks are error.
    t = new Task(job.generateTaskId(2, nodeId + "2"), 2, nodeId + "2", ExecutionStatus.ERROR);
    job.updateTaskStatus(t);
    Assert.assertEquals(ExecutionStatus.ERROR, waitAllDecider.checkJobStatus(job));
  }

  @Test
  public void testHasEnoughTaskExecutor() {
    OfflineJob job = new OfflineJob(1, topic, numberOfPartition, replicationFactor);
    Assert.assertTrue(waitAllDecider.hasEnoughTaskExecutorsToStart(job, partitionAssignment), "Not enough executors");

    partitionAssignment.removePartition(1);
    List<Instance> instances = createInstances(replicationFactor - 1);
    partitionAssignment.addPartition(new Partition(1, instances, Collections.emptyList(), Collections.emptyList()));

    Assert.assertFalse(waitAllDecider.hasEnoughTaskExecutorsToStart(job, partitionAssignment),
        "Partition-1 miss one replica, decider should return false to indicate no enough task executors.");

    partitionAssignment.removePartition(1);
    instances = createInstances(replicationFactor);

    try {
      partitionAssignment.addPartition(
          new Partition(numberOfPartition + 1, instances, instances, Collections.emptyList()));
      waitAllDecider.hasEnoughTaskExecutorsToStart(job, partitionAssignment);
      Assert.fail("Invalid partition id, decider should throw an exception.");
    } catch (VeniceException e) {
      //expected
    }

    partitionAssignment.removePartition(numberOfPartition + 1);
    Assert.assertFalse(waitAllDecider.hasEnoughTaskExecutorsToStart(job, partitionAssignment),
        "Partition number is smaller than required, decider should return false.");
    partitionAssignment = new PartitionAssignment(topic, numberOfPartition);
    Assert.assertFalse(waitAllDecider.hasEnoughTaskExecutorsToStart(job, partitionAssignment),
        "Partition number is smaller than required, decider should return false.");
  }
}
