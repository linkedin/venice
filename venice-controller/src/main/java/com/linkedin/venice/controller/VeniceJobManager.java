package com.linkedin.venice.controller;

import com.linkedin.venice.controlmessage.ControlMessageHandler;
import com.linkedin.venice.controlmessage.StoreStatusMessage;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.job.ExecutionStatus;
import com.linkedin.venice.job.Job;
import com.linkedin.venice.job.JobRepository;
import com.linkedin.venice.job.OfflineJob;
import com.linkedin.venice.job.Task;
import com.linkedin.venice.meta.MetadataRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.log4j.Logger;


/**
 * Venice job manager to handle all of control messages to update job/task status and do actions based the status
 * change.
 */
public class VeniceJobManager implements ControlMessageHandler<StoreStatusMessage> {
  private static final Logger logger = Logger.getLogger(VeniceJobManager.class);
  private final JobRepository jobRepository;
  private final MetadataRepository metadataRepository;
  private final AtomicInteger idGenerator = new AtomicInteger(0);
  private final int epoch;

  public VeniceJobManager(int epoch, JobRepository jobRepository, MetadataRepository metadataRepository) {
    this.epoch = epoch;
    this.jobRepository = jobRepository;
    this.metadataRepository = metadataRepository;
  }

  public void checkAllExistingJobs() {
    //In some cases, all of tasks status was updated, but controller failed when updating the whole job status.
    //After controller setting up, check all existing job to terminated them if needed.
    List<Job> jobs = jobRepository.getAllRunningJobs();
    for (Job job : jobs) {
      ExecutionStatus status = job.checkJobStatus();
      if (status.equals(ExecutionStatus.COMPLETED)) {
        handleJobComplete(job);
      } else if (status.equals(ExecutionStatus.ERROR)) {
        handleJobError(job);
      }
    }
  }

  public synchronized void startOfflineJob(String kafkaTopic, int numberOfPartition, int replicaFactor) {
    OfflineJob job = new OfflineJob(this.generateJobId(), kafkaTopic, numberOfPartition, replicaFactor);
    try {
      jobRepository.startJob(job);
    } catch (Exception e) {
      logger.error(
          "Can not refresh a offline job for kafka topic:" + kafkaTopic + " with number of partition:" + numberOfPartition
              + " and replica factor:" + replicaFactor, e);
      jobRepository.stopJobWithError(job.getJobId(), job.getKafkaTopic());
    }
  }

  public synchronized ExecutionStatus getOfflineJobStatus(String kafkaTopic) {
    List<Job> jobs;
    jobs = jobRepository.getRunningJobOfTopic(kafkaTopic);
    if (jobs.isEmpty()) {
      //Can not find job in running jobs. Then find it in terminated jobs.
      jobs = jobRepository.getTerminatedJobOfTopic(kafkaTopic);
    }
    if (jobs.size() > 1) {
      throw new VeniceException(
          "There should be only one job for each kafka topic. But now there are:" + jobs.size() + " jobs running.");
    } else if (jobs.size() <= 0) {
      return ExecutionStatus.NOT_CREATED;
    }

    Job job = jobs.get(0);
    return job.getStatus();
  }

  @Override
  public void handleMessage(StoreStatusMessage message) {
    List<Job> jobs = jobRepository.getRunningJobOfTopic(message.getKafkaTopic());
    // We should avoid update tasks in same kafka topic in the same time. Different kafka topics could be accessed concurrently.
    synchronized (jobs) {
      // TODO Right now, for offline-push, there is only one job running for each version. Should be change to get job
      // by job Id when H2V can get job Id and send it through kafka control message.
      if (jobs.size() > 1) {
        throw new VeniceException(
            "There should be only one job running for each kafka topic. But now there are:" + jobs.size()
                + " jobs running.");
      }
      Job job = jobs.get(0);
      if (!job.getStatus().equals(ExecutionStatus.STARTED)) {

        throw new VeniceException(
            "Can not handle message:" + message.getMessageId() + ". Job has not been started:" + generateJobId());
      }
      //Update task status at first.
      Task task =
          new Task(job.generateTaskId(message.getPartitionId(), message.getInstanceId()), message.getPartitionId(),
              message.getInstanceId(), message.getStatus());
      jobRepository.updateTaskStatus(job.getJobId(), job.getKafkaTopic(), task);
      logger.info("Update status of Task:" + task.getTaskId() + " to status:" + task.getStatus());
      //Check the job status after updating task status to see is the whole job completed or error.
      ExecutionStatus status = job.checkJobStatus();
      if (status.equals(ExecutionStatus.COMPLETED)) {
        logger.info("All of task are completed, mark job as completed too.");
        handleJobComplete(job);
      } else if (status.equals(ExecutionStatus.ERROR)) {
        logger.info("Some of tasks are failed, mark job as failed too.");
        handleJobError(job);
      }
    }
  }

  private void handleJobComplete(Job job) {
    //Do the swap. Change version to active so that router could get the notification and sending the message to this version.
    Store store = metadataRepository.getStore(Version.parseStoreFromKafkaTopicName(job.getKafkaTopic()));
    int versionNumber = Version.parseVersionFromKafkaTopicName(job.getKafkaTopic());
    store.updateVersionStatus(versionNumber, VersionStatus.ACTIVE);
    if (versionNumber > store.getCurrentVersion()) {
      store.setCurrentVersion(versionNumber);
    }
    for (int retry = 2; retry >= 0; retry--) {
      try {
        metadataRepository.updateStore(store);
        break;
      } catch (Exception e) {
        logger.error("Can not activate version: " + versionNumber + " for store: " + store.getName());
        // TODO: Reconcile inconsistency between version and job in case of fatal failure
      }
    }
    jobRepository.stopJob(job.getJobId(), job.getKafkaTopic());
  }

  private void handleJobError(Job job) {
    //TODO do the roll back here.
    jobRepository.stopJobWithError(job.getJobId(), job.getKafkaTopic());
  }

  /**
   * When the version is retired, archive related jobs.
   *
   * @param kafkaTopic
   */
  public synchronized void archiveJobs(String kafkaTopic) {
    List<Job> jobs = jobRepository.getTerminatedJobOfTopic(kafkaTopic);
    List<Long> jobIds = jobs.stream().map(Job::getJobId).collect(Collectors.toList());
    for (Long jobId : jobIds) {
      jobRepository.archiveJob(jobId, kafkaTopic);
    }
  }

  /**
   * Id is composed by two part of integers. The higher 32 bytes is the epoch number and the lower 32 bytes is the
   * integer generated by AtomicInteger in this process. When this process(controller) is failed, the AtomicInteger will
   * start from 0 again. But a new epoch number should be used to ensure there is not duplicated id.
   * <p>
   * TODO Use some Linkedin common solution to get the global id here.
   *
   * @return
   */
  public long generateJobId() {
    long id = epoch;
    id = id << 32;
    int generatedId = idGenerator.incrementAndGet();
    id = id | generatedId;
    return id;
  }

  public JobRepository getJobRepository() {
    return jobRepository;
  }

  public MetadataRepository getMetadataRepository() {
    return metadataRepository;
  }
}
