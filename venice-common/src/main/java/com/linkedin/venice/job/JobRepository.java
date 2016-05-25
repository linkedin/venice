package com.linkedin.venice.job;

import com.linkedin.venice.VeniceResource;
import com.linkedin.venice.meta.Partition;
import java.util.List;
import java.util.Map;


/**
 * Interface that defines the read operations to access jobs and tasks.
 */
public interface JobRepository extends VeniceResource {
  /**
   * Get the running job for given kafka topic. Currently, only one job should be running for one kafatopic/Helix
   * resource. But in the further, a off-line and a near-line maybe running in a same time.
   *
   * @param kafkaTopic
   *
   * @return
   */
  public List<Job> getRunningJobOfTopic(String kafkaTopic);

  public List<Job> getAllRunningJobs();

  public List<Job> getTerminatedJobOfTopic(String kafkaTopic);

  public void archiveJob(long jobId, String kafkaTopic);

  public void updateTaskStatus(long jobId, String kafkaTopic, Task task);

  /**
   * Give the partitions which is assigned to this job and update if they are different from what job knew before.
   *
   * @param jobId
   * @param kafkaTopic
   * @param partitions
   */
  public void updateJobExecutors(long jobId, String kafkaTopic, Map<Integer, Partition> partitions);

  public void stopJob(long jobId, String kafkaTopic);

  public void stopJobWithError(long jobId, String kafkaTopic);

  public void startJob(Job job);

  public ExecutionStatus getJobStatus(long jobId, String kafkaTopic);

  public Job getJob(long jobId, String kafkaTopic);

  public void subscribeJobStatusChange(String kafkaTopic, JobStatusChangedListener listener);

  public void unsubscribeJobStatusChange(String kafkaTopic, JobStatusChangedListener listener);

  interface JobStatusChangedListener{
    /**
     * Handle job status change event.
     * @param job The changed job.
     */
    void onJobStatusChange(Job job);
  }
}
