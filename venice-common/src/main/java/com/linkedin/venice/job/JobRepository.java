package com.linkedin.venice.job;

import java.util.List;
import java.util.Set;


/**
 * Created by yayan on 3/7/16.
 */
public interface JobRepository {
  /**
   * Get the running job for given kafka topic. Currently, only one job should be running for one kafatopic/Helix
   * resource. But in the further, a off-line and a near-line maybe running in a same time.
   *
   * @param kafkaTopic
   *
   * @return
   */
  public List<Job> getRunningJobOfTopic(String kafkaTopic);

  public List<Job> getTerminatedJobOfTopic(String kafkaTopic);

  public void archiveJob(long jobId, String kafkaTopic);

  public void updateTaskStatus(long jobId, String kafkaTopic, Task task);

  public void stopJob(long jobId, String kafkaTopic);

  public void stopJobWithError(long jobId, String kafkaTopic);

  public void startJob(Job job);

  public ExecutionStatus getJobStatus(long jobId, String kafkaTopic);

  public Job getJob(long jobId, String kafkaTopic);

  public void clear();
}
