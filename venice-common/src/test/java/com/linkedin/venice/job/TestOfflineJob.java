package com.linkedin.venice.job;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.Partition;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Test cases for Offline Job.
 */
public class TestOfflineJob {
  private String topic = "testTopic";
  private int numberOfPartition = 4;
  private int replicaFactor = 3;
  private String nodeId = "localhost_1234";
  private Map<Integer, Partition> partitions = new HashMap<>();

  @BeforeMethod
  public void setup() {
    for (int i = 0; i < numberOfPartition; i++) {
      List<Instance> instances = new ArrayList<>();
      for (int j = 0; j < replicaFactor; j++) {
        Instance instance = new Instance(nodeId + j, "localhost", 1235);
        instances.add(instance);
      }
      Partition partition = new Partition(i, topic, instances);
      partitions.put(i, partition);
    }
  }

  @AfterMethod
  public void cleanup() {
    partitions.clear();
  }

  private void testValidateJobTransition(Job job, ExecutionStatus[] validStatus, ExecutionStatus[] invalidStatus) {
    ExecutionStatus currentStatus = job.getStatus();
    //valid status transition
    for (ExecutionStatus status : validStatus) {
      try {
        job.validateStatusTransition(status);
      } catch (VeniceException e) {
        Assert.fail("Job status:" + currentStatus.toString() + " should be able " + status.toString());
      }
    }
    //invalid status transition.
    for (ExecutionStatus status : invalidStatus) {
      try {
        job.validateStatusTransition(status);
        Assert.fail("Job status:" + currentStatus.toString() + " should not be able " + status.toString());
      } catch (VeniceException e) {

      }
    }
  }

  @Test
  public void validateJobStartedTransition() {
    OfflineJob job = new OfflineJob(1, topic, numberOfPartition, replicaFactor);
    job.setStatus(ExecutionStatus.STARTED);
    ExecutionStatus[] validStatus = new ExecutionStatus[]{ExecutionStatus.ERROR, ExecutionStatus.COMPLETED};
    ExecutionStatus[] invalidStatus =
        new ExecutionStatus[]{ExecutionStatus.PROGRESS, ExecutionStatus.ARCHIVED, ExecutionStatus.STARTED};
    testValidateJobTransition(job, validStatus, invalidStatus);
  }

  @Test
  public void validateJobCompletedTransition() {
    OfflineJob job = new OfflineJob(1, topic, numberOfPartition, replicaFactor);
    job.setStatus(ExecutionStatus.COMPLETED);
    ExecutionStatus[] validStatus = new ExecutionStatus[]{ExecutionStatus.ARCHIVED};
    ExecutionStatus[] invalidStatus =
        new ExecutionStatus[]{ExecutionStatus.STARTED, ExecutionStatus.COMPLETED, ExecutionStatus.ERROR, ExecutionStatus.NEW};
    testValidateJobTransition(job, validStatus, invalidStatus);
  }

  @Test
  public void validateJobErrorTransition() {
    OfflineJob job = new OfflineJob(1, topic, numberOfPartition, replicaFactor);
    job.setStatus(ExecutionStatus.ERROR);
    ExecutionStatus[] validStatus = new ExecutionStatus[]{ExecutionStatus.ARCHIVED};
    ExecutionStatus[] invalidStatus =
        new ExecutionStatus[]{ExecutionStatus.STARTED, ExecutionStatus.COMPLETED, ExecutionStatus.ERROR, ExecutionStatus.NEW};
    testValidateJobTransition(job, validStatus, invalidStatus);
  }

  @Test
  public void validateJobArchivedTransition() {
    OfflineJob job = new OfflineJob(1, topic, numberOfPartition, replicaFactor);
    job.setStatus(ExecutionStatus.ARCHIVED);
    ExecutionStatus[] validStatus = new ExecutionStatus[]{};
    ExecutionStatus[] invalidStatus =
        new ExecutionStatus[]{ExecutionStatus.ARCHIVED, ExecutionStatus.STARTED, ExecutionStatus.COMPLETED, ExecutionStatus.ERROR, ExecutionStatus.NEW};
    testValidateJobTransition(job, validStatus, invalidStatus);
  }

  @Test
  public void testUpdateTaskStatus() {
    OfflineJob job = new OfflineJob(1, topic, numberOfPartition, replicaFactor);
    job.updateExecutingTasks(partitions);
    Task task = new Task(job.generateTaskId(0, nodeId + "1"), 0, nodeId + "1", ExecutionStatus.STARTED);
    job.updateTaskStatus(task);
    Assert.assertEquals(task.getStatus(), job.getTaskStatus(0, task.getTaskId()),
        "Status is not corrected after updating");

    task.setStatus(ExecutionStatus.COMPLETED);
    Assert
        .assertEquals(ExecutionStatus.STARTED, job.getTaskStatus(0, task.getTaskId()), "Status should not be updated.");

    job.updateTaskStatus(task);
    Assert.assertEquals(ExecutionStatus.COMPLETED, job.getTaskStatus(0, task.getTaskId()), "Status should be updated.");

    task.setStatus(ExecutionStatus.STARTED);
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
    job.updateExecutingTasks(partitions);
    Task task = new Task(job.generateTaskId(0, nodeId + "1"), 0, nodeId + "1", ExecutionStatus.COMPLETED);
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
    job.updateExecutingTasks(partitions);
    Task task = new Task(job.generateTaskId(0, nodeId + "1"), 0, nodeId + "1", ExecutionStatus.STARTED);

    job.updateTaskStatus(task);

    task.setStatus(ExecutionStatus.COMPLETED);
    job.updateTaskStatus(task);
    try {
      job.updateTaskStatus(task);
      Assert.fail("A VeniceException should be thrown because task is already terminated.");
    } catch (VeniceException e) {
      //expected.
    }
  }
}
