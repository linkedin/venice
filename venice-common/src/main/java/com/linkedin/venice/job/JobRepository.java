package com.linkedin.venice.job;

import java.util.List;


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

  public void archiveJob(long jobId);

  public void updateTaskStatus(long jobId, Task task);

  public void stopJob(long jobId, boolean isError);

  public void startJob(Job job);

  public JobAndTaskStatus getJobStatus(long jobId);

  public Job getJob(long jobId);

  public void lock();

  public void unlock();

  public void clear();
}
