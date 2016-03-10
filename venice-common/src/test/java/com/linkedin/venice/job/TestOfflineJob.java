package com.linkedin.venice.job;

import com.linkedin.venice.exceptions.VeniceException;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Test cases for Offline Job.
 */
public class TestOfflineJob {
  private long jobId = 1;
  private String topic = "testTopic";
  private int numberOfPartition = 4;
  private int replicaFactor = 3;

  @Test
  public void testUpdateTaskStatus() {
    OfflineJob job = new OfflineJob(1, topic, numberOfPartition, replicaFactor);

    Task task = new Task("t1", 0, "i1", JobAndTaskStatus.STARTED);
    job.updateTaskStatus(task);
    Assert.assertEquals(task.getStatus(), job.getTaskStatus(0, "t1"), "Status is not corrected after updating");

    task.setStatus(JobAndTaskStatus.COMPLETED);
    Assert.assertEquals(JobAndTaskStatus.STARTED, job.getTaskStatus(0, "t1"), "Status should not be updated.");

    job.updateTaskStatus(task);
    Assert.assertEquals(JobAndTaskStatus.COMPLETED, job.getTaskStatus(0, "t1"), "Status should be updated.");

    task.setStatus(JobAndTaskStatus.STARTED);
    try {
      job.updateTaskStatus(task);
      Assert.fail("A VeniceException should be thrown because task is already started.");
    } catch (VeniceException e) {
      //expected.
    }
  }

  @Test
  public void testUpdateNotStartedTaskStatus() {
    OfflineJob job = new OfflineJob(1, topic, numberOfPartition, replicaFactor);

    Task task = new Task("t1", 0, "i1", JobAndTaskStatus.COMPLETED);
    try {
      job.updateTaskStatus(task);
      Assert.fail("Task should be updated to Started at first, then to other status");
    } catch (VeniceException e) {
      //expected
    }
  }

  @Test
  public void testUpdateTerminatedTaskStatus() {
    OfflineJob job = new OfflineJob(1, topic, numberOfPartition, replicaFactor);

    Task task = new Task("t1", 0, "i1", JobAndTaskStatus.STARTED);
    job.updateTaskStatus(task);

    task.setStatus(JobAndTaskStatus.COMPLETED);
    job.updateTaskStatus(task);
    try {
      job.updateTaskStatus(task);
      Assert.fail("A VeniceException should be thrown because task is already terminated.");
    } catch (VeniceException e) {
      //expected.
    }
  }

  @Test
  public void testCheckJobStatusNotRunning() {
    OfflineJob job = new OfflineJob(1, topic, numberOfPartition, replicaFactor);

    try {
      job.checkJobStatus();
      Assert.fail("Job is not running.");
    } catch (VeniceException e) {
      //expected
    }
  }

  @Test
  public void testCheckJobStatus() {
    OfflineJob job = new OfflineJob(1, topic, numberOfPartition, replicaFactor);
    job.setStatus(JobAndTaskStatus.STARTED);

    Assert.assertEquals(JobAndTaskStatus.STARTED, job.checkJobStatus(),
        "Did not get any updates. SHould still be in running status.");

    for (int i = 0; i < numberOfPartition; i++) {
      for (int j = 0; j < replicaFactor; j++) {
        Task t = new Task(i + "_" + j, i, String.valueOf(j), JobAndTaskStatus.STARTED);
        job.updateTaskStatus(t);
      }
    }
    Assert.assertEquals(JobAndTaskStatus.STARTED, job.checkJobStatus(),
        "All of tasks are just started, should be in runing status.");

    for (int i = 0; i < numberOfPartition; i++) {
      for (int j = 0; j < replicaFactor; j++) {
        Task t = new Task(i + "_" + j, i, String.valueOf(j), JobAndTaskStatus.COMPLETED);
        if (i == 1 && j == 2) {
          continue;
        }
        job.updateTaskStatus(t);
      }
    }
    Task t = new Task("1_2", 1, "2", JobAndTaskStatus.PROGRESS);
    job.updateTaskStatus(t);
    //Only one task is not terminated
    Assert.assertEquals(JobAndTaskStatus.STARTED, job.checkJobStatus(),
        "There is still one task in progress status, job is still running.");

    t.setStatus(JobAndTaskStatus.COMPLETED);
    job.updateTaskStatus(t);
    //Only one task is not terminated
    Assert.assertEquals(JobAndTaskStatus.COMPLETED, job.checkJobStatus(), "All the tasks are completed, job is completed.");
  }

  @Test
  public void testCheckJobStatusWhenJobFail() {
    OfflineJob job = new OfflineJob(1, topic, numberOfPartition, replicaFactor);
    job.setStatus(JobAndTaskStatus.STARTED);
    for (int i = 0; i < numberOfPartition; i++) {
      for (int j = 0; j < replicaFactor; j++) {
        Task t = new Task(i + "_" + j, i, String.valueOf(j), JobAndTaskStatus.STARTED);
        job.updateTaskStatus(t);
        if (i == 2 && j == 0) {
          continue;
        }
        t.setStatus(JobAndTaskStatus.COMPLETED);
        job.updateTaskStatus(t);
      }
    }

    Task t = new Task("2_0", 2, "0", JobAndTaskStatus.PROGRESS);
    job.updateTaskStatus(t);
    //Only one task is not terminated
    Assert.assertEquals(JobAndTaskStatus.STARTED, job.checkJobStatus(),
        "There is still one task in progress status, job is still running.");
    t.setStatus(JobAndTaskStatus.ERROR);
    job.updateTaskStatus(t);
    //Only one task is not terminated
    Assert.assertEquals(JobAndTaskStatus.ERROR, job.checkJobStatus(),
        "One task is failed, based on current strategy, job should be failed.");
  }
}
