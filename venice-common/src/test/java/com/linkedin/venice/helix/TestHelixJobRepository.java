package com.linkedin.venice.helix;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.job.JobAndTaskStatus;
import com.linkedin.venice.job.OfflineJob;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Test cases for HelixJobRepository
 */
public class TestHelixJobRepository {
  private HelixJobRepository repository;

  @BeforeMethod
  public void setup() {
    repository = new HelixJobRepository();
    repository.start();
  }

  @AfterMethod
  public void cleanup() {
    repository.clear();
  }

  @Test
  public void testStartJob() {
    OfflineJob job = new OfflineJob(1, "topic1", 1, 1);
    repository.startJob(job);
    Assert.assertEquals(job, repository.getJob(1), "Can not get correct job from repository after starting the job.");
    Assert.assertEquals(JobAndTaskStatus.STARTED, repository.getJobStatus(1), "Job should be running after being started.");
  }

  @Test
  public void testStopJob() {
    OfflineJob job = new OfflineJob(1, "topic1", 1, 1);
    repository.startJob(job);

    repository.stopJob(1, false);
    Assert.assertEquals(job, repository.getJob(1), "Can not get correct job from repository after starting the job.");
    Assert.assertEquals(JobAndTaskStatus.COMPLETED, repository.getJobStatus(1), "Job should be completed after being stopped.");

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
      repository.stopJob(1, false);
      Assert.fail("Job is not started.");
    } catch (VeniceException e) {
      //expected
    }
  }

  @Test
  public void testStopNotRunningJob() {
    OfflineJob job = new OfflineJob(1, "topic1", 1, 1);
    repository.startJob(job);
    job.setStatus(JobAndTaskStatus.COMPLETED);
    try {
      repository.stopJob(1, false);
      Assert.fail("Job is not in running status.");
    } catch (VeniceException e) {
      //expected
    }
  }

  @Test
  public void testArchiveJob() {
    OfflineJob job = new OfflineJob(1, "topic1", 1, 1);
    repository.startJob(job);
    repository.stopJob(1, false);
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
}
