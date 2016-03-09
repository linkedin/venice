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
  public Job getRunningJob(String kafkaTopic);

  public List<Task> getTasks(String jobId, int partitionId);

  public void archiveJob(String jobId);

  public void updateTaskStatus(String taskId, TaskStatus status);

  public void stopJob(String jobId);

  public void startJob(Job job);

}
