package com.linkedin.venice.helix;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.job.ExecutionStatus;
import com.linkedin.venice.job.OfflineJob;
import com.linkedin.venice.job.Task;
import com.linkedin.venice.utils.ZkServerWrapper;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.zookeeper.CreateMode;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Test cases for HelixJobRepository
 */
public class TestHelixJobRepository {
  private String zkAddress;
  private ZkClient zkClient;
  private String cluster = "test-job-cluster";
  private String clusterPath = "/test-job-cluster";
  private String jobPath = "/jobs";
  private HelixAdapterSerializer adapter = new HelixAdapterSerializer();
  private ZkServerWrapper zkServerWrapper;
  private HelixJobRepository repository;

  @BeforeMethod
  public void setup() {
    zkServerWrapper = ZkServerWrapper.getZkServer();
    zkAddress = zkServerWrapper.getZkAddress();
    zkClient = new ZkClient(zkAddress, ZkClient.DEFAULT_SESSION_TIMEOUT, ZkClient.DEFAULT_CONNECTION_TIMEOUT, adapter);
    zkClient.create(clusterPath, null, CreateMode.PERSISTENT);
    zkClient.create(clusterPath + jobPath, null, CreateMode.PERSISTENT);

    repository = new HelixJobRepository(zkClient, adapter, cluster);
    repository.start();
  }

  @AfterMethod
  public void cleanup() {
    zkClient.deleteRecursive(clusterPath);
    zkClient.close();
    zkServerWrapper.close();
    repository.clear();
  }

  @Test
  public void testStartJob() {
    OfflineJob job = new OfflineJob(1, "topic1", 1, 1);
    repository.startJob(job);
    Assert.assertEquals(job, repository.getJob(1), "Can not get correct job from repository after starting the job.");
    Assert.assertEquals(ExecutionStatus.STARTED, repository.getJobStatus(1),
        "Job should be running after being started.");
  }

  @Test
  public void testStopJob() {
    OfflineJob job = new OfflineJob(1, "topic1", 1, 1);
    repository.startJob(job);

    repository.stopJob(1);
    Assert.assertEquals(job, repository.getJob(1), "Can not get correct job from repository after starting the job.");
    Assert.assertEquals(ExecutionStatus.COMPLETED, repository.getJobStatus(1),
        "Job should be completed after being stopped.");

    try {
      repository.getRunningJobOfTopic("topic1");
      Assert.fail("Job is stooped, should not exist in running job list.");
    } catch (VeniceException e) {
      //expected
    }
  }

  @Test
  public void testStopNotStartedJob() {
    OfflineJob job = new OfflineJob(1, "topic1", 1, 1);
    try {
      repository.stopJob(1);
      Assert.fail("Job is not started.");
    } catch (VeniceException e) {
      //expected
    }
  }

  @Test
  public void testStopNotRunningJob() {
    OfflineJob job = new OfflineJob(1, "topic1", 1, 1);
    repository.startJob(job);
    job.setStatus(ExecutionStatus.COMPLETED);
    try {
      repository.stopJob(1);
      Assert.fail("Job is not in running status.");
    } catch (VeniceException e) {
      //expected
    }
  }

  @Test
  public void testArchiveJob() {
    OfflineJob job = new OfflineJob(1, "topic1", 1, 1);
    repository.startJob(job);
    repository.stopJob(1);
    repository.archiveJob(1);
    try {
      repository.getJob(1);
      Assert.fail("Job is archived, should not exist in repository right now.");
    } catch (VeniceException e) {
      //expected
    }
  }

  @Test
  public void testArchiveNotCompleteJob() {
    OfflineJob job = new OfflineJob(1, "topic1", 1, 1);
    repository.startJob(job);
    try {
      repository.archiveJob(1);
      Assert.fail("Job is not terminated, can not be archived.");
    } catch (VeniceException e) {
      //expected
    }
  }

  @Test
  public void testLoadFromZk() {
    int jobCount = 3;
    int numberOfPartition = 10;
    int replicaFactor = 3;
    for (int i = 0; i < jobCount; i++) {
      OfflineJob job = new OfflineJob(i, "topic" + i, numberOfPartition, replicaFactor);
      repository.startJob(job);
    }

    Task t1 = new Task("t1", 3, "instance1", ExecutionStatus.STARTED);
    //Start one task for job0 and job1
    for (int i = 0; i < jobCount - 1; i++) {
      repository.updateTaskStatus(i, t1);
    }
    //Failed one task for job1
    Task t2 = new Task("t1", 3, "instance2", ExecutionStatus.ERROR);
    repository.updateTaskStatus(1, t2);

    for (int i = 0; i < numberOfPartition; i++) {
      for (int j = 0; j < replicaFactor; j++) {
        Task task = new Task(i + "_" + j, i, "test", ExecutionStatus.STARTED);
        repository.updateTaskStatus(2, task);
        task = new Task(i + "_" + j, i, "test", ExecutionStatus.COMPLETED);
        repository.updateTaskStatus(2, task);
      }
    }
    HelixJobRepository newRepository = new HelixJobRepository(zkClient, adapter, cluster);
    newRepository.start();
    Assert.assertEquals(newRepository.getJob(0).getTaskStatus(3, "t1"), ExecutionStatus.STARTED,
        "Task dose not be loaded correctly.");
    Assert.assertEquals(newRepository.getJob(1).getTaskStatus(3, "t1"), ExecutionStatus.ERROR,
        "Task dose not be loaded correctly.");
    Assert.assertEquals(newRepository.getJob(2).getTaskStatus(1, "1_1"), ExecutionStatus.COMPLETED,
        "Task dose not be loaded correctly.");

    Assert.assertEquals(newRepository.getJobStatus(0), ExecutionStatus.STARTED,
        "Job should be started and updated to ZK");
    Assert.assertEquals(newRepository.getJobStatus(1), ExecutionStatus.ERROR,
        "Job should be failed because one of task is failed.");
    Assert.assertEquals(newRepository.getJobStatus(2), ExecutionStatus.COMPLETED,
        "Job should be started and updated to ZK");
    newRepository.clear();
  }
}
