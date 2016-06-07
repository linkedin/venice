package com.linkedin.venice.controller;

import com.linkedin.venice.controlmessage.ControlMessageHandler;
import com.linkedin.venice.controlmessage.StoreStatusMessage;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.job.ExecutionStatus;
import com.linkedin.venice.job.Job;
import com.linkedin.venice.job.JobRepository;
import com.linkedin.venice.job.OfflineJob;
import com.linkedin.venice.job.Task;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.log4j.Logger;


/**
 * Venice job manager to handle all of control messages to update job/task status and do actions based the status
 * change.
 */
public class VeniceJobManager implements ControlMessageHandler<StoreStatusMessage>, RoutingDataRepository.RoutingDataChangedListener, JobRepository.JobStatusChangedListener {
  private static final Logger logger = Logger.getLogger(VeniceJobManager.class);
  private final JobRepository jobRepository;
  private final RoutingDataRepository routingDataRepository;
  private final ReadWriteStoreRepository metadataRepository;
  private final AtomicInteger idGenerator = new AtomicInteger(0);
  private final int epoch;
  private Admin helixAdmin;
  private final String clusterName;
  private ConcurrentMap<String, Job> waitingJobMap;

  public VeniceJobManager(String clusterName, int epoch, JobRepository jobRepository,
      ReadWriteStoreRepository metadataRepository, RoutingDataRepository routingDataRepository) {
    this.clusterName = clusterName;
    this.epoch = epoch;
    this.jobRepository = jobRepository;
    this.metadataRepository = metadataRepository;
    this.routingDataRepository = routingDataRepository;
    waitingJobMap = new ConcurrentHashMap<>();
  }

  public void setAdmin(Admin helixAdmin) {
    this.helixAdmin = helixAdmin;
  }

  public void checkAllExistingJobs() {
    //In some cases, all of tasks status was updated, but controller failed when updating the whole job status.
    //After controller setting up, check all existing job to terminated them if needed.
    List<Job> jobs = jobRepository.getAllRunningJobs();
    for (Job job : jobs) {
      ExecutionStatus status = job.checkJobStatus();
      if (status.equals(ExecutionStatus.COMPLETED)) {
        jobRepository.stopJob(job.getJobId(), job.getKafkaTopic());
      } else if (status.equals(ExecutionStatus.ERROR)) {
        jobRepository.stopJobWithError(job.getJobId(), job.getKafkaTopic());
      } else if (status.equals(ExecutionStatus.STARTED)) {
        // If job is still running, take over this job including refresh executors' information, subscribe status change event etc.
        routingDataRepository.subscribeRoutingDataChange(job.getKafkaTopic(), this);
        // subscribe the status change of this job.
        jobRepository.subscribeJobStatusChange(job.getKafkaTopic(), this);
        // Sync up with routing data again to avoid missing some change during the controller's failure.
        jobRepository.updateJobExecutors(job.getJobId(), job.getKafkaTopic(),
            routingDataRepository.getPartitions(job.getKafkaTopic()));
      } else {
        throw new VeniceException("Invalid status:" + status.toString() + " for job:" + job.getJobId());
      }
    }
  }

  public void startOfflineJob(String kafkaTopic, int numberOfPartition, int replicaFactor) {
    OfflineJob job = new OfflineJob(this.generateJobId(), kafkaTopic, numberOfPartition, replicaFactor);
    try {
      waitUntilJobStart(job);
      jobRepository.startJob(job);
      jobRepository.subscribeJobStatusChange(kafkaTopic,this);
    } catch (Exception e) {
      logger.error("Can not refresh a offline job for kafka topic:" + kafkaTopic + " with number of partition:"
          + numberOfPartition + " and replica factor:" + replicaFactor, e);
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
      if (jobs.size() != 1) {
        throw new VeniceException(
            "There should be only one job running for kafka topic:" + message.getKafkaTopic() + ". But now there are:"
                + jobs.size() + " jobs running.");
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
        jobRepository.stopJob(job.getJobId(), job.getKafkaTopic());
      } else if (status.equals(ExecutionStatus.ERROR)) {
        logger.info("Some of tasks are failed, mark job as failed too.");
        jobRepository.stopJobWithError(job.getJobId(), job.getKafkaTopic());
      }
    }
  }

  private void updateStoreVersionStatus(Job job, Store store, VersionStatus status) {
    int versionNumber = Version.parseVersionFromKafkaTopicName(job.getKafkaTopic());
    store.updateVersionStatus(versionNumber, status);

    if (status == VersionStatus.ACTIVE) {
      if (versionNumber > store.getCurrentVersion()) {
        store.setCurrentVersion(versionNumber);
      } else {
        logger.warn("Ignoring older version job complete message. Version " + versionNumber +
            " Store " + store.getName() + " Topic " + job.getKafkaTopic());
      }
    }
  }

  // TODO : instead of running when the store version completes, it should run from
  // a timer job/service which runs every few hours.
  private void deleteOldStoreVersions(Store store) {
    if (helixAdmin == null) {
      return;
    }
    // TODO : versions to preserveLastFew must read from config
    final int NUM_VERSIONS_TO_PRESERVE = 2;
    List<Version> versionsToDelete = store.retrieveVersionsToDelete(NUM_VERSIONS_TO_PRESERVE);
    for (Version version : versionsToDelete) {
      helixAdmin.deleteOldStoreVersion(clusterName, version.kafkaTopicName());
      store.deleteVersion(version.getNumber());
    }
  }

  private void handleJobComplete(Job job) {
    routingDataRepository.unSubscribeRoutingDataChange(job.getKafkaTopic(), this);
    jobRepository.unsubscribeJobStatusChange(job.getKafkaTopic(), this);
    //Do the swap. Change version to active so that router could get the notification and sending the message to this version.
    Store store = metadataRepository.getStore(Version.parseStoreFromKafkaTopicName(job.getKafkaTopic()));
    updateStoreVersionStatus(job, store, VersionStatus.ACTIVE);
    for (int retry = 2; retry >= 0; retry--) {
      try {
        metadataRepository.updateStore(store);
        logger.info("Activate version:" + job.getKafkaTopic() + " for store:" + store.getName());
        break;
      } catch (Exception e) {
        int versionNumber = Version.parseVersionFromKafkaTopicName(job.getKafkaTopic());
        logger.error("Can not activate version: " + versionNumber + " for store: " + store.getName());
        // TODO: Reconcile inconsistency between version and job in case of fatal failure
      }
    }
    deleteOldStoreVersions(store);
  }

  private void handleJobError(Job job) {
    //TODO do the roll back here.
    routingDataRepository.unSubscribeRoutingDataChange(job.getKafkaTopic(), this);
    jobRepository.unsubscribeJobStatusChange(job.getKafkaTopic(), this);
    Store store = metadataRepository.getStore(Version.parseStoreFromKafkaTopicName(job.getKafkaTopic()));
    updateStoreVersionStatus(job, store, VersionStatus.ERROR);
    deleteOldStoreVersions(store);
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

  /**
   * When we creating a new job for new helix resource. We need to know how many tasks we need to create and which
   * instance is assigned to execute this task. Unfortunately controller need some time to wait all of participants
   * assigned to this resource become ONLINE, then get these information from external view, otherwise the external view
   * is empty or uncompleted.
   *
   * @param job
   */
  private void waitUntilJobStart(Job job) {
    //There are no enough partitions and/or replicas to execute tasks. Wait until all of replicas becoming online
    synchronized (job) {
      waitingJobMap.put(job.getKafkaTopic(), job);
      routingDataRepository.subscribeRoutingDataChange(job.getKafkaTopic(), this);
      if (routingDataRepository.containsKafkaTopic(job.getKafkaTopic())) {
        if (areEnoughExecutors(job, routingDataRepository.getPartitions(job.getKafkaTopic()))) {
          logger.info("Job:" + job.getJobId() + " could be started.");
          return;
        }
      }
      // Wait until getting enough nodes to execute job.
      try {
        logger.info("Wait job:" + job.getJobId() + " being started.");
        //TODO add a timeout to avoid wait for ever. And set status to ERROR when time out.
        job.wait();
        waitingJobMap.remove(job.getKafkaTopic());
        logger.info("Job:" + job.getJobId() + " could be started.");
      } catch (InterruptedException e) {
        throw new VeniceException("Met error when wait job being started.", e);
      }
    }
  }

  private boolean areEnoughExecutors(Job job, Map<Integer, Partition> partitions) {
    try {
      job.updateExecutingPartitions(partitions);
      return true;
    } catch (VeniceException e) {
      return false;
    }
  }

  @Override
  public void onJobStatusChange(Job job) {
    if (job.getStatus().equals(ExecutionStatus.ERROR)) {
      handleJobError(job);
    } else if (job.getStatus().equals(ExecutionStatus.COMPLETED)) {
      handleJobComplete(job);
    }
  }

  @Override
  public void onRoutingDataChanged(String kafkaTopic, Map<Integer, Partition> partitions) {
    Job job = waitingJobMap.get(kafkaTopic);
    if (job != null) {
      // Some job is waiting for starting.
      synchronized (job) {
        if (areEnoughExecutors(job, partitions)) {
          logger.info("Get enough executor for job:" + job.getJobId() + " Continue running.");
          job.notify();
        }
      }
    } else {
      // Update executors for running jobs
      List<Job> jobs = jobRepository.getRunningJobOfTopic(kafkaTopic);
      if (jobs.isEmpty()) {
        return;
      }
      for (Job runningJob : jobs) {
        jobRepository.updateJobExecutors(runningJob.getJobId(), kafkaTopic, partitions);
      }
    }
  }
}
