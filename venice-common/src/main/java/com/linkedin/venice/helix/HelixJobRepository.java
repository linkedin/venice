package com.linkedin.venice.helix;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.job.Job;
import com.linkedin.venice.job.ExecutionStatus;
import com.linkedin.venice.job.JobRepository;
import com.linkedin.venice.job.Task;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.validation.constraints.NotNull;


/**
 * Job Repository which persist job data on Helix.
 * <p>
 * The transition of Job's state machine:
 * <p>
 * <ul> <li>NEW->STARTED</li> <li>STARTED->COMPLETED</li> <li>STARTED->ERROR</li> <li>COMPLETED->ARCHIVED</li>
 * <li>ERROR->COMPLETED</li> </ul>
 */
public class HelixJobRepository implements JobRepository {
  private Map<Long, Job> jobMap;
  private Map<String, List<Job>> topicToJobsMap;

  public HelixJobRepository() {
    jobMap = new HashMap<>();
    topicToJobsMap = new HashMap<>();
  }

  @Override
  public synchronized List<Job> getRunningJobOfTopic(@NotNull String kafkaTopic) {
    List<Job> jobs = topicToJobsMap.get(kafkaTopic);

    if (jobs == null) {
      //No running jobs in this topic.
      throw new VeniceException("Can not find running job for kafka topic:" + kafkaTopic);
    }
    return jobs;
  }

  @Override
  public synchronized void archiveJob(long jobId) {
    Job job = this.getJob(jobId);
    if (job.getStatus().equals(ExecutionStatus.COMPLETED) || job.getStatus().equals(ExecutionStatus.ERROR)) {
      job.setStatus(ExecutionStatus.ARCHIVED);
      this.jobMap.remove(jobId);
    } else {
      throw new VeniceException("Job:" + jobId + " is in " + job.getStatus() + " status. Can not be archived.");
    }
  }

  @Override
  public synchronized void updateTaskStatus(long jobId, @NotNull Task task) {
    Job job = this.getJob(jobId);
    job.updateTaskStatus(task);
  }

  @Override
  public synchronized void stopJob(long jobId) {
    internalStopJob(jobId, ExecutionStatus.COMPLETED);
  }

  @Override
  public synchronized void stopJobWithError(long jobId) {
    internalStopJob(jobId, ExecutionStatus.ERROR);
  }

  private void internalStopJob(long jobId, ExecutionStatus status) {
    Job job = this.getJob(jobId);
    if (job.getStatus().equals(ExecutionStatus.STARTED)) {
      job.setStatus(status);
      List<Job> jobs = this.topicToJobsMap.get(job.getKafkaTopic());
      for (int i = 0; i < jobs.size(); i++) {
        if (jobs.get(i).getJobId() == jobId) {
          jobs.remove(i);
          break;
        }
      }
      if (jobs.isEmpty()) {
        this.topicToJobsMap.remove(job.getKafkaTopic());
      }
    } else {
      throw new VeniceException("Job:" + jobId + " is in " + job.getStatus() + ". Can not be stopped.");
    }
  }

  @Override
  public synchronized void startJob(@NotNull Job job) {
    if (!job.getStatus().equals(ExecutionStatus.NEW)) {
      throw new VeniceException("Job:" + job.getJobId() + " is in " + job.getStatus() + ". Can not be started.");
    }
    job.setStatus(ExecutionStatus.STARTED);
    this.jobMap.put(job.getJobId(), job);
    List<Job> jobs = this.topicToJobsMap.get(job.getKafkaTopic());
    if (jobs == null) {
      jobs = new ArrayList<>();
      topicToJobsMap.put(job.getKafkaTopic(), jobs);
    }
    jobs.add(job);
  }

  @Override
  public synchronized ExecutionStatus getJobStatus(long jobId) {
    Job job = this.getJob(jobId);
    return job.getStatus();
  }

  @Override
  public synchronized Job getJob(long jobId) {
    Job job = jobMap.get(jobId);
    if (job == null) {
      throw new VeniceException("Job:" + jobId + " dose not exist.");
    }
    return job;
  }

  //TODO connect to helix.

  public synchronized void start() {

  }

  //TODO clean up related helix resource.
  public synchronized void clear() {
    this.jobMap.clear();
    this.topicToJobsMap.clear();
  }
}
