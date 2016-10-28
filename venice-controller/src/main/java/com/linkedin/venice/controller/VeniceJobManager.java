package com.linkedin.venice.controller;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.job.ExecutionStatus;
import com.linkedin.venice.job.Job;
import com.linkedin.venice.job.JobRepository;
import com.linkedin.venice.job.JobStatusDecider;
import com.linkedin.venice.job.OfflineJob;
import com.linkedin.venice.job.Task;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.status.StatusMessageHandler;
import com.linkedin.venice.status.StoreStatusMessage;
import java.util.ArrayList;
import java.util.HashMap;
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
public class VeniceJobManager implements StatusMessageHandler<StoreStatusMessage>, RoutingDataRepository.RoutingDataChangedListener, JobRepository.JobStatusChangedListener {
  private static final Logger logger = Logger.getLogger(VeniceJobManager.class);
  private final JobRepository jobRepository;
  private final RoutingDataRepository routingDataRepository;
  private final ReadWriteStoreRepository metadataRepository;
  private final AtomicInteger idGenerator = new AtomicInteger(0);
  private final int epoch;
  private VeniceHelixAdmin helixAdmin;
  private final String clusterName;
  private ConcurrentMap<String, Job> waitingJobMap;

  private final long jobWaitTimeInMilliseconds;

  public VeniceJobManager(String clusterName, int epoch, JobRepository jobRepository,
      ReadWriteStoreRepository metadataRepository, RoutingDataRepository routingDataRepository,
      long jobWaitTimeInMilliseconds) {
    this.clusterName = clusterName;
    this.epoch = epoch;
    this.jobRepository = jobRepository;
    this.metadataRepository = metadataRepository;
    this.routingDataRepository = routingDataRepository;
    this.jobWaitTimeInMilliseconds = jobWaitTimeInMilliseconds;
    waitingJobMap = new ConcurrentHashMap<>();
  }

  public void setAdmin(VeniceHelixAdmin helixAdmin) {
    this.helixAdmin = helixAdmin;
  }

  public void checkAllExistingJobs() {
    //In some cases, all of tasks status was updated, but controller failed when updating the whole job status.
    //After controller setting up, check all existing job to terminated them if needed.
    List<Job> jobs = jobRepository.getAllRunningJobs();
    for (Job job : jobs) {
      ExecutionStatus status = getJobStatusDecider(job).checkJobStatus(job);
      if (status.equals(ExecutionStatus.COMPLETED)) {
        jobRepository.stopJob(job.getJobId(), job.getKafkaTopic());
      } else if (status.equals(ExecutionStatus.ERROR)) {
        jobRepository.stopJobWithError(job.getJobId(), job.getKafkaTopic());
      } else if (status.equals(ExecutionStatus.STARTED)) {
        if (routingDataRepository.containsKafkaTopic(job.getKafkaTopic())) {
          // If job is still running, take over this job including refresh executors' information, subscribe status change event etc.
          routingDataRepository.subscribeRoutingDataChange(job.getKafkaTopic(), this);
          // subscribe the status change of this job.
          jobRepository.subscribeJobStatusChange(job.getKafkaTopic(), this);
          // Sync up with routing data again to avoid missing some change during the controller's failure.
          if (getJobStatusDecider(job).hasMinimumTaskExecutorsToKeepRunning(job,
              routingDataRepository.getPartitionAssignments(job.getKafkaTopic()))) {
            jobRepository.updateJobExecutingTasks(job.getJobId(),
                routingDataRepository.getPartitionAssignments(job.getKafkaTopic()));
          } else {
            // Can not get enough task executors, stop job and un-register listeners
            logger.error("Job:" + job.getJobId() + " for topic:" + job.getKafkaTopic()
                + " dose not have enough replicas. Stop it and mark as ERROR status.");
            routingDataRepository.unSubscribeRoutingDataChange(job.getKafkaTopic(), this);
            jobRepository.unsubscribeJobStatusChange(job.getKafkaTopic(), this);
            jobRepository.stopJobWithError(job.getJobId(), job.getKafkaTopic());
          }
        } else {
          // Topic does not exist, stop this job.
          logger.error("Kafka topic" + job.getKafkaTopic() + " dose not exist. Stop job:" + job.getJobId());
          jobRepository.stopJobWithError(job.getJobId(), job.getKafkaTopic());
        }
      } else {
        throw new VeniceException("Invalid status:" + status.toString() + " for job:" + job.getJobId());
      }
    }
  }

  public void startOfflineJob(String kafkaTopic, int numberOfPartition, int replicaFactor, OfflinePushStrategy strategy) {
    OfflineJob job = new OfflineJob(this.generateJobId(), kafkaTopic, numberOfPartition, replicaFactor, strategy);
    logger.info("Starting job:" + job.getJobId() + " for topic:" + job.getKafkaTopic());
    try {
      waitUntilJobStart(job);
      // Subscribe at first to prevent missing some event happen just after job is started.
      jobRepository.subscribeJobStatusChange(kafkaTopic,this);
      jobRepository.startJob(job);
      logger.info("Job has been started. Id:" + job.getJobId() + " topic:" + job.getKafkaTopic());
    } catch (Exception e) {
      String errorMsg = "Can not start an offline job for kafka topic:" + kafkaTopic + " with number of partition:"
          + numberOfPartition + " and replica factor:" + replicaFactor;
      logger.error(errorMsg, e);
      try {
        jobRepository.stopJobWithError(job.getJobId(), job.getKafkaTopic());
      } catch (Exception e1) {
        logger.error("Can not put job:" + job.getJobId() + " for topic:" + job.getKafkaTopic() + " to error status.", e1);
        // Because we will clean up related resource after a job becoming ERROR, so here we need to clean up them manually once
        // we can not set the job status to ERROR.
        cleanUpFailedJob(job);
      }
      throw new VeniceException(errorMsg, e);
    }
  }

  //TODO after job is complete, return status of the version.
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

  public synchronized Map<String, Long> getOfflineJobProgress(String kafkaTopic) {
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
      logger.warn("Could not find job for topic: " + kafkaTopic + ".  Reporting progress of 0");
      return new HashMap<>();
    }

    Job job = jobs.get(0);
    return job.getProgress();
  }

  public static long aggregateProgress(Map<String, Long> progressMap) {
    long totalProgress = 0L;
    for (long progress : progressMap.values()){
      totalProgress += progress;
    }
    return totalProgress;
  }

  @Override
  public void handleMessage(StoreStatusMessage message) {
    logger.info("Received message from:" + message.getInstanceId() + " for topic:" + message.getKafkaTopic());
    logger.debug("Message content:" + message.toString());
    List<Job> jobs = jobRepository.getRunningJobOfTopic(message.getKafkaTopic());
    // We should avoid update tasks in same kafka topic in the same time. Different kafka topics could be accessed concurrently.
    synchronized (jobs) {
      // TODO Right now, for offline-push, there is only one job running for each version. Should be change to get job
      // by job Id when H2V can get job Id and send it through kafka control message.  UPDATE: NOT using job ID anymore.
      if (jobs.size() == 0) {
        // Can not find the running job for the status message, try to find a terminated job.
        // For example, if Venice use wait n-1 strategy, job might be terminated before the last one replica becoming
        // online. So controller will still receiving the message from the last replica even if the job has been
        // terminated before.
        List<Job> terminatedJobs = jobRepository.getTerminatedJobOfTopic(message.getKafkaTopic());
        synchronized (terminatedJobs) {
          if (terminatedJobs.size() == 1) {
            // Just ignore the message, because its job has been termianted before.
            logger.info("Received a message for the terminated job of topic:" + message.getKafkaTopic() + ", status:"
                + message.getStatus() + " from:" + message.getInstanceId() + ". Just ignore it.");
            return;
          }
        }
      } else if (jobs.size() == 1) {
        //Find only one running job for the given topic.
        Job job = jobs.get(0);
        //Update task status at first.
        String taskId = job.generateTaskId(message.getPartitionId(), message.getInstanceId());
        Task task =
            new Task(taskId, message.getPartitionId(), message.getInstanceId(), message.getStatus());
        Task oldTask = jobRepository.getJob(job.getJobId(), job.getKafkaTopic()).getTask(message.getPartitionId(), taskId);
        task.setProgress(oldTask.getProgress());
        if (message.getStatus().equals(ExecutionStatus.PROGRESS) || message.getStatus().equals(ExecutionStatus.COMPLETED)){
          if (message.getOffset() > 0L) {
            task.setProgress(message.getOffset());
          }
        }
        jobRepository.updateTaskStatus(job.getJobId(), job.getKafkaTopic(), task);
        logger.info("Update status of Task:" + task.getTaskId()
            + " to status:" + task.getStatus()
            + " with progress: " +task.getProgress()
            + " for topic:" + message.getKafkaTopic());
        //Check the job status after updating task status to see is the whole job completed or error.
        ExecutionStatus status = getJobStatusDecider(job).checkJobStatus(job);
        if (status.equals(ExecutionStatus.COMPLETED)) {
          logger.info("All of task are completed, mark job:" + job.getJobId() + " for topic:" + job.getKafkaTopic()
              + " as completed too.");
          jobRepository.stopJob(job.getJobId(), job.getKafkaTopic());
        } else if (status.equals(ExecutionStatus.ERROR)) {
          logger.info("Some of tasks are failed, mark job:" + job.getJobId() + " for topic:" + job.getKafkaTopic()
              + " as failed too.");
          jobRepository.stopJobWithError(job.getJobId(), job.getKafkaTopic());
        }
        return;
      }
      // Can not find a correct running or terminated start job.
      throw new VeniceException(
          "Can not handle this message because no proper job is found. kafka topic:" + message.getKafkaTopic());
    }
  }

  private void updateStoreVersionStatus(Job job, Store store, VersionStatus status) {
    int versionNumber = Version.parseVersionFromKafkaTopicName(job.getKafkaTopic());
    store.updateVersionStatus(versionNumber, status);
    logger.info("Updated store"+store.getName()+" version:"+versionNumber+" to status:"+status.toString());

    if (status == VersionStatus.ONLINE) {
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
  private void cleanUpStore(Store store) {
    if (helixAdmin == null) {
      return;
    }
    // TODO : versions to preserveLastFew must read from config
    final int NUM_VERSIONS_TO_PRESERVE = 2;
    List<Version> versionsToDelete = store.retrieveVersionsToDelete(NUM_VERSIONS_TO_PRESERVE);
    for (Version version : versionsToDelete) {
      deleteOneStoreVersion(store, version.getNumber());
      logger.info("Deleted store:" + store.getName() + " version:" + version.getNumber());
      helixAdmin.getTopicManager().deleteTopic(version.kafkaTopicName());
    }
  }

  /***
   * Delete the version specified from the store, kill the running ingestion, remove the helix resource, and update zookeeper.
   * @param store
   * @param versionNumber
   */
  private void deleteOneStoreVersion(Store store, int versionNumber){
    String resourceName = new Version(store.getName(), versionNumber).kafkaTopicName();
    helixAdmin.deleteHelixResource(clusterName, resourceName);
    helixAdmin.killOfflineJob(clusterName, resourceName);
    store.deleteVersion(versionNumber);
    metadataRepository.updateStore(store);
  }

  private void cleanUpFailedJob(Job job) {
    logger.info("Clean up resources related to job:" + job.getJobId() + " topic:" + job.getKafkaTopic());
    Store store = metadataRepository.getStore(Version.parseStoreFromKafkaTopicName(job.getKafkaTopic()));
    updateStoreVersionStatus(job, store, VersionStatus.ERROR);
    metadataRepository.updateStore(store);
    if (helixAdmin == null) {
      return;
    }
    int versionNumber = Version.parseVersionFromKafkaTopicName(job.getKafkaTopic());
    deleteOneStoreVersion(store, versionNumber);
    logger.info("Deleted store:" + store.getName() + " version:" + versionNumber);
    // Check the feature flag to decide whether manager would delete the topic for failed job or not.
    if (helixAdmin.getVeniceHelixResource(clusterName).getConfig().isEnableTopicDeletionWhenJobFailed()) {
      helixAdmin.getTopicManager().deleteTopic(job.getKafkaTopic());
      logger.info("Deleted topic:" + job.getKafkaTopic());
    } else {
      logger.info("Topic deletion is disabled for this controller. Ignore deletion request.");
    }
  }

  private void handleJobComplete(Job job) {
    routingDataRepository.unSubscribeRoutingDataChange(job.getKafkaTopic(), this);
    jobRepository.unsubscribeJobStatusChange(job.getKafkaTopic(), this);
    //Do the swap. Change version to online so that router could get the notification and sending the message to this version.
    Store store = metadataRepository.getStore(Version.parseStoreFromKafkaTopicName(job.getKafkaTopic()));
    VersionStatus newStatus;
    if (store.isPaused()) {
      newStatus = VersionStatus.PUSHED;
    } else {
      newStatus = VersionStatus.ONLINE;
    }
    updateStoreVersionStatus(job, store, newStatus);
    try {
      // We havealready implemented retry in updateStore.
      metadataRepository.updateStore(store);
      logger.info(
          "Update version:" + job.getKafkaTopic() + " for store:" + store.getName() + " to status:" + newStatus);
    } catch (Exception e) {
      int versionNumber = Version.parseVersionFromKafkaTopicName(job.getKafkaTopic());
      logger.error("Can not set version: " + versionNumber + " to online for store: " + store.getName());
      // TODO: Reconcile inconsistency between version and job in case of fatal failure
    }
    cleanUpStore(store);
  }

  private void handleJobError(Job job) {
    //TODO do the roll back here.
    routingDataRepository.unSubscribeRoutingDataChange(job.getKafkaTopic(), this);
    jobRepository.unsubscribeJobStatusChange(job.getKafkaTopic(), this);
    cleanUpFailedJob(job);
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
   * Whether job will fail after applying the given partition assignment.
   */
  public boolean willJobFail(String kafkaTopic, PartitionAssignment PartitionAssignment) {
    List<Job> jobs = jobRepository.getRunningJobOfTopic(kafkaTopic);
    if (jobs.isEmpty()) {
      //the job has been termianted.
      return false;
    }
    if (jobs.size() > 1) {
      throw new VeniceException(
          "There should be only one job for each kafka topic. But now there are:" + jobs.size() + " jobs running.");
    }
    Job job = jobs.get(0);
    return !getJobStatusDecider(job).hasMinimumTaskExecutorsToKeepRunning(job, PartitionAssignment);
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
      // Read current partition and decide whether there are enough assigned replicas at first. It avoids the case:
      // Before subscribing the listener to routing data, all replicas have already been assigned, so will not get any
      // notification.
      if (routingDataRepository.containsKafkaTopic(job.getKafkaTopic()) && getJobStatusDecider(
          job).hasEnoughTaskExecutorsToStart(job, routingDataRepository.getPartitionAssignments(job.getKafkaTopic()))) {
        // Job has not been updated to ZK. So we update it's local copy at first. Then they will be update to ZK when starting the job.
        job.updateExecutingTasks(routingDataRepository.getPartitionAssignments(job.getKafkaTopic()));
        waitingJobMap.remove(job.getKafkaTopic());
        logger.info("Job:" + job.getJobId() + " topic:" + job.getKafkaTopic() + " could be started.");
        return;
      }
      // Wait until getting enough nodes to execute job.
      try {
        long startTime = System.currentTimeMillis();
        long nextWaitTime = jobWaitTimeInMilliseconds;
        while (System.currentTimeMillis() - startTime < jobWaitTimeInMilliseconds) {
          logger.info("Wait on starting job:"+job.getJobId() +" topic:"+job.getKafkaTopic());
          job.wait(nextWaitTime);
          long spentTime = System.currentTimeMillis() - startTime;
          // In order to avoid incorrect notification, judge whether there are enough executors.
          if (routingDataRepository.containsKafkaTopic(job.getKafkaTopic()) && getJobStatusDecider(
              job).hasEnoughTaskExecutorsToStart(job, routingDataRepository.getPartitionAssignments(job.getKafkaTopic()))) {
            job.updateExecutingTasks(routingDataRepository.getPartitionAssignments(job.getKafkaTopic()));
            waitingJobMap.remove(job.getKafkaTopic());
            logger.info("Wait is completed on job:" + job.getJobId() + " topic:" + job.getKafkaTopic() + " wait time:"
                + spentTime);
            logger.info("Job:" + job.getJobId() + " topic:" + job.getKafkaTopic() + " could be started.");
            return;
          } else {
            // Incorrect notification, wait again.
            nextWaitTime = jobWaitTimeInMilliseconds - spentTime;
          }
        }
        //Time out. Job has not been updated to ZK. So do not need to mark as ERROR status, Invoker will get this exception which indicates it's failed to start job.
        throw new VeniceException("Waiting is time out on job:" + job.getJobId() + " topic:" + job.getKafkaTopic()
            + ". Start job is failed.");
      } catch (InterruptedException e) {
        throw new VeniceException("Met error when wait job to be started. Start job is failed.", e);
      }
    }
  }

  private JobStatusDecider getJobStatusDecider(Job job) {
    Store store = metadataRepository.getStore(Version.parseStoreFromKafkaTopicName(job.getKafkaTopic()));
    OfflinePushStrategy strategy = store.getOffLinePushStrategy();
    return JobStatusDecider.getDecider(strategy);
  }

  @Override
  public void onJobStatusChange(Job job) {
    logger.info(
        "Get job status changed notification. Job:" + job.getJobId() + "topic:" + job.getKafkaTopic() + " status:" + job
            .getStatus());
    if (job.getStatus().equals(ExecutionStatus.ERROR)) {
      handleJobError(job);
    } else if (job.getStatus().equals(ExecutionStatus.COMPLETED)) {
      handleJobComplete(job);
    }
  }

  @Override
  public void onRoutingDataChanged(PartitionAssignment partitionAssignment) {
    logger.info(
        "Get routing data changed notification for topic:" + partitionAssignment.getTopic() + " partitions size:" + partitionAssignment.getAssignedNumberOfPartitions());
    Job job = waitingJobMap.get(partitionAssignment.getTopic());
    if (job != null) {
      // Some job is waiting for starting.
      synchronized (job) {
        if (getJobStatusDecider(job).hasEnoughTaskExecutorsToStart(job, partitionAssignment)) {
          logger.info("Get enough executor for job:" + job.getJobId() + " Continue running.");
          job.notify();
        }
        // If there are enough executors or some of the executors are failed before job starting, do not notify the waiting job. At last the
        // waiting job will be time out and throw exception
      }
    } else {
      // Update executors for running jobs. Create new list here to avoid concurrent modification.
      List<Job> jobs = new ArrayList<>(jobRepository.getRunningJobOfTopic(partitionAssignment.getTopic()));
      if (jobs.isEmpty()) {
        return;
      }
      // Fail the job if the partition mapping was changed.  enough replicas here to decide whether stop job or not in the future.
      for (Job runningJob : jobs) {
        if (getJobStatusDecider(runningJob).hasMinimumTaskExecutorsToKeepRunning(runningJob, partitionAssignment)) {
          // Job has been started and updated to zk. So we need to update ZK by through job repository.
          jobRepository.updateJobExecutingTasks(runningJob.getJobId(), partitionAssignment);
        } else {
          // New partition mapping indicates that some nodes are failed so there are not enough replicas. In that case
          // we need to fail the whole job.  Note that we could have different policies to judge "enough replicas".
          // So here it does not mean that job should be failed after ONE node is failed. Please see method
          // hasEnoughExecutors for more details.
          logger.error("Partition allocation is changed, but there are not enough replicas for job" + runningJob.getJobId()
              + ". Stop it and mark it as ERROR status.");
          jobRepository.stopJobWithError(runningJob.getJobId(), runningJob.getKafkaTopic());
        }
      }
    }
  }

  @Override
  public void onRoutingDataDeleted(String kafkaTopic) {
    Job job = waitingJobMap.get(kafkaTopic);

    if (job != null) {
      logger.info("Topic:" + kafkaTopic + " is deleted. Cancel the waiting job related to this topic.");
      jobRepository.stopJobWithError(job.getJobId(), job.getKafkaTopic());
    } else {
      List<Job> jobs = jobRepository.getRunningJobOfTopic(kafkaTopic);
      if (jobs.size() == 1) {
        logger.info("Topic:" + kafkaTopic + " is deleted. Cancel the running job related to this topic.");
        jobRepository.stopJobWithError(jobs.get(0).getJobId(), kafkaTopic);
      } else {
        logger.warn("Topic:" + kafkaTopic + " is deleted. But can not find the job related to this topic.");
      }
    }
  }
}
