package com.linkedin.venice.helix;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.job.Job;
import com.linkedin.venice.job.JobAndTaskStatus;
import com.linkedin.venice.job.JobRepository;
import com.linkedin.venice.job.Task;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.validation.constraints.NotNull;


/**
 * Job Repository which persist job data on Helix.
 */
public class HelixJobRepository implements JobRepository {
  private Map<Long, Job> jobMap;
  private Map<String, List<Job>> topicToJobMap;

  private ReadWriteLock lock = new ReentrantReadWriteLock();

  public HelixJobRepository() {
    jobMap = new HashMap<>();
    topicToJobMap = new HashMap<>();
  }

  @Override
  public List<Job> getRunningJobOfTopic(@NotNull String kafkaTopic) {
    lock.readLock().lock();
    try {
      List<Job> jobs = topicToJobMap.get(kafkaTopic);

      if (jobs == null) {
        //No running jobs in this topic.
        throw new VeniceException("Can not find running job for kafka topic:" + kafkaTopic);
      }
      return jobs;
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public void archiveJob(long jobId) {
    lock.writeLock().lock();
    try {
      Job job = this.getJob(jobId);
      if (job.getStatus().equals(JobAndTaskStatus.COMPLETED) || job.getStatus().equals(JobAndTaskStatus.ERROR)) {
        job.setStatus(JobAndTaskStatus.ARCHIVED);
        this.jobMap.remove(jobId);
      } else {
        throw new VeniceException("Job:" + jobId + " is in " + job.getStatus() + " status. Can not be archived.");
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void updateTaskStatus(long jobId, @NotNull Task task) {
    lock.writeLock().lock();
    try {
      Job job = this.getJob(jobId);
      job.updateTaskStatus(task);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void stopJob(long jobId, boolean isError) {
    lock.writeLock().lock();
    try {
      Job job = this.getJob(jobId);
      if (job.getStatus().equals(JobAndTaskStatus.STARTED)) {
        job.setStatus(isError ? JobAndTaskStatus.ERROR : JobAndTaskStatus.COMPLETED);
        List<Job> jobs = this.topicToJobMap.get(job.getKafkaTopic());
        for (int i = 0; i < jobs.size(); i++) {
          if (jobs.get(i).getJobId() == jobId) {
            jobs.remove(i);
            break;
          }
        }
        if(jobs.isEmpty()){
          this.topicToJobMap.remove(job.getKafkaTopic());
        }
      } else {
        throw new VeniceException("Job:" + jobId + " is in " + job.getStatus() + ". Can not be stopped.");
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public synchronized void startJob(@NotNull Job job) {
    lock.writeLock().lock();
    try {
      if (!job.getStatus().equals(JobAndTaskStatus.UNKNOW)) {
        throw new VeniceException("Job:" + job.getJobId() + " is in " + job.getStatus() + ". Can not be started.");
      }
      job.setStatus(JobAndTaskStatus.STARTED);
      this.jobMap.put(job.getJobId(), job);
      List<Job> jobs = this.topicToJobMap.get(job.getKafkaTopic());
      if (jobs == null) {
        jobs = new ArrayList<>();
        topicToJobMap.put(job.getKafkaTopic(), jobs);
      }
      jobs.add(job);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public synchronized JobAndTaskStatus getJobStatus(long jobId) {
    lock.readLock().lock();
    try {
      Job job = this.getJob(jobId);
      return job.getStatus();
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public synchronized Job getJob(long jobId) {
    lock.readLock().lock();
    try {
      Job job = jobMap.get(jobId);
      if (job == null) {
        throw new VeniceException("Job:" + jobId + " dose not exist.");
      }
      return job;
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public void lock() {
    lock.writeLock().lock();
  }

  @Override
  public void unlock() {
    lock.writeLock().unlock();
  }

  //TODO connect to helix.

  public void start() {

  }

  //TODO clean up related helix resource.
  public void clear() {
    this.jobMap.clear();
    this.topicToJobMap.clear();
  }
}
