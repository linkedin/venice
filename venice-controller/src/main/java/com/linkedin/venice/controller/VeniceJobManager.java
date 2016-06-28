package com.linkedin.venice.controller;

import com.linkedin.venice.controlmessage.ControlMessageHandler;
import com.linkedin.venice.controlmessage.StoreStatusMessage;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.job.ExecutionStatus;
import com.linkedin.venice.job.Job;
import com.linkedin.venice.job.JobRepository;
import com.linkedin.venice.job.OfflineJob;
import com.linkedin.venice.job.Task;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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

  //TODO read from configuration
  private final long jobWaitTimeInMilliseconds = 15000;

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
        if (hasEnoughExecutors(job, routingDataRepository.getPartitions(job.getKafkaTopic()))) {
          jobRepository.updateJobExecutingTasks(job.getJobId(), job.getKafkaTopic(),
              routingDataRepository.getPartitions(job.getKafkaTopic()));
        } else {
          jobRepository.stopJobWithError(job.getJobId(), job.getKafkaTopic());
        }
      } else {
        throw new VeniceException("Invalid status:" + status.toString() + " for job:" + job.getJobId());
      }
    }
  }

  public void startOfflineJob(String kafkaTopic, int numberOfPartition, int replicaFactor) {
    OfflineJob job = new OfflineJob(this.generateJobId(), kafkaTopic, numberOfPartition, replicaFactor);
    try {
      waitUntilJobStart(job);
      // Subscribe at first to prevent missing some event happen just after job is started.
      jobRepository.subscribeJobStatusChange(kafkaTopic,this);
      jobRepository.startJob(job);
    } catch (Exception e) {
      String errorMsg = "Can not start an offline job for kafka topic:" + kafkaTopic + " with number of partition:"
          + numberOfPartition + " and replica factor:" + replicaFactor;
      logger.error(errorMsg, e);
      try {
        jobRepository.stopJobWithError(job.getJobId(), job.getKafkaTopic());
      } catch (Exception e1) {
        logger.error("Can not put job:" + job.getJobId() + " for topic:" + job.getKafkaTopic() + " to error status.");
      }
      throw new VeniceException(errorMsg, e);
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
      deleteOneStoreVersion(store, version.getNumber());
    }
  }

  /***
   * Delete the version specified from the store, remove the helix resource, and update zookeeper.
   * @param store
   * @param versionNumber
   */
  private void deleteOneStoreVersion(Store store, int versionNumber){
    helixAdmin.deleteOldStoreVersion(clusterName, new Version(store.getName(), versionNumber).kafkaTopicName());
    store.deleteVersion(versionNumber);
    metadataRepository.updateStore(store);
  }

  /***
   * Delete all kafka topics for this store that correspond to a version older than
   * still exists for the store.
   */
  private void deleteOldKafkaTopics(Store store){
    if (helixAdmin == null) {
      // Some old tests does not set helixAdmin memeber. So add a condition here to avoid fail tests.
      return;
    }
    Optional<Integer> minAvailableVersion = store.getVersions().stream() /* all available versions */
        .map(version -> version.getNumber()) /* version numbers */
        .min(Comparator.<Integer>naturalOrder()); /* min available */
    if (minAvailableVersion.isPresent()){
      helixAdmin.getTopicManager()
          .deleteTopicsForStoreOlderThanVersion(store.getName(), minAvailableVersion.get());
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
    deleteOldKafkaTopics(store);
  }

  private void handleJobError(Job job) {
    //TODO do the roll back here.
    routingDataRepository.unSubscribeRoutingDataChange(job.getKafkaTopic(), this);
    jobRepository.unsubscribeJobStatusChange(job.getKafkaTopic(), this);
    Store store = metadataRepository.getStore(Version.parseStoreFromKafkaTopicName(job.getKafkaTopic()));
    updateStoreVersionStatus(job, store, VersionStatus.ERROR);
    if (helixAdmin == null) {
      // TODO need to remove Some old tests does not set helixAdmin memeber. So add a condition here to avoid fail tests.
      return;
    }
    deleteOneStoreVersion(store, Version.parseVersionFromKafkaTopicName(job.getKafkaTopic()));
    helixAdmin.getTopicManager()
        .deleteTopic(job.getKafkaTopic());
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
      // Read current partition and decide whether there are enough assigned replicas at first. It avoids the case:
      // Before subscribing the listener to routing data, all replicas have already been assigned, so will not get any
      // notification.
      if (routingDataRepository.containsKafkaTopic(job.getKafkaTopic()) && hasEnoughExecutors(job,
          routingDataRepository.getPartitions(job.getKafkaTopic()))) {
        // Job has not been updated to ZK. So we update it's local copy at first. Then they will be update to ZK when starting the job.
        job.updateExecutingTasks(routingDataRepository.getPartitions(job.getKafkaTopic()));
        waitingJobMap.remove(job.getKafkaTopic());
        logger.info("Job:" + job.getJobId() + " topic:" + job.getKafkaTopic() + " could be started.");
        return;
      }
      // Wait until getting enough nodes to execute job.
      try {
        long startTime = System.currentTimeMillis();
        long nextWaitTime = jobWaitTimeInMilliseconds;
        while (System.currentTimeMillis() - startTime < jobWaitTimeInMilliseconds) {
          job.wait(nextWaitTime);
          long spentTime = System.currentTimeMillis() - startTime;
          // In order to avoid incorrect notification, judge whether there are enough executors.
          if (routingDataRepository.containsKafkaTopic(job.getKafkaTopic()) && hasEnoughExecutors(job,
              routingDataRepository.getPartitions(job.getKafkaTopic()))) {
            job.updateExecutingTasks(routingDataRepository.getPartitions(job.getKafkaTopic()));
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

  private boolean hasEnoughExecutors(Job job, Map<Integer, Partition> partitions) {
    if (partitions.size() != job.getNumberOfPartition()) {
      logger.info("Number of partitions:" + partitions.size() + " is different from the number required when job was created:" +
          job.getNumberOfPartition());
      return false;
    }

    Store store = metadataRepository.getStore(Version.parseStoreFromKafkaTopicName(job.getKafkaTopic()));
    OfflinePushStrategy strategy = store.getOffLinePushStrategy();
    for (Partition partition : partitions.values()) {
      int replicaNumberDifference = job.getReplicaFactor() - partition.getInstances().size();
      if (job.getReplicaFactor() == 1 || strategy.equals(OfflinePushStrategy.WAIT_ALL_REPLICAS)) {
        // If only only one replica is required, the situation is as same as WAIT_ALL_REPLICAS strategy.
        if (replicaNumberDifference > 0) {
          //Some of nodes are failed.
          logger.info("Replica factor:" + partition.getInstances().size() + " in partition:" + partition.getId()
              + "is smaller from the required factor:" + job.getReplicaFactor());
          return false;
        }
      } else if (strategy.equals(OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION)) {
        if (replicaNumberDifference > 1) {
          //More then one nodes are failed in this partition
          logger.info("Replica factor:" + partition.getInstances().size() + " in partition:" + partition.getId()
              + "is smaller from the required factor:" + (job.getReplicaFactor() - 1));
          return false;
        }
      } else {
        throw new VeniceException("Do not support offline push strategy:" + strategy);
      }
    }
    return true;
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
  public void onRoutingDataChanged(String kafkaTopic, Map<Integer, Partition> partitions) {
    logger.info(
        "Get routing data changed notification for topic:" + kafkaTopic + " partitions size:" + partitions.size());
    Job job = waitingJobMap.get(kafkaTopic);
    if (job != null) {
      // Some job is waiting for starting.
      synchronized (job) {
        if (hasEnoughExecutors(job, partitions)) {
          logger.info("Get enough executor for job:" + job.getJobId() + " Continue running.");
          job.notify();
        }
        // If there are enough executors or some of the executors are failed before job starting, do not notify the waiting job. At last the
        // waiting job will be time out and throw exception
      }
    } else {
      // Update executors for running jobs. Create new list here to avoid concurrent modification.
      List<Job> jobs = new ArrayList<>(jobRepository.getRunningJobOfTopic(kafkaTopic));
      if (jobs.isEmpty()) {
        return;
      }
      // Fail the job if the partition mapping was changed.  enough replicas here to decide whether stop job or not in the future.
      for (Job runningJob : jobs) {
        if (hasEnoughExecutors(runningJob, partitions)) {
          // Job has been started and updated to zk. So we need to update ZK by through job repository.
          jobRepository.updateJobExecutingTasks(runningJob.getJobId(), kafkaTopic, partitions);
        } else {
          // New partition mapping indicates that some nodes are failed so there are not enough replicas. In that case
          // we need to fail the whole job.  Note that we could have different policies to judge "enough replicas".
          // So here it does not mean that job should be failed after ONE node is failed. Please see method
          // hasEnoughExecutors for more details.
          jobRepository.stopJobWithError(runningJob.getJobId(), runningJob.getKafkaTopic());
        }
      }
    }
  }
}
