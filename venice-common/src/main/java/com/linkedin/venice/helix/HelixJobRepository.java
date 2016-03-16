package com.linkedin.venice.helix;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.job.Job;
import com.linkedin.venice.job.ExecutionStatus;
import com.linkedin.venice.job.JobRepository;
import com.linkedin.venice.job.OfflineJob;
import com.linkedin.venice.job.Task;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.RoutingDataChangedListener;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.meta.VeniceSerializer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.validation.constraints.NotNull;
import org.apache.helix.AccessOption;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.log4j.Logger;


/**
 * Job Repository which persist job data on Helix.
 * <p>
 * The transition of Job's state machine:
 * <p>
 * <ul> <li>NEW->STARTED</li> <li>STARTED->COMPLETED</li> <li>STARTED->ERROR</li> <li>COMPLETED->ARCHIVED</li>
 * <li>ERROR->COMPLETED</li> </ul>
 */
public class HelixJobRepository implements JobRepository, RoutingDataChangedListener {
  private static final Logger logger = Logger.getLogger(HelixJobRepository.class);
  private Map<Long, Job> jobMap;
  private Map<String, List<Job>> topicToJobsMap;

  private final ZkBaseDataAccessor<OfflineJob> jobDataAccessor;

  private final ZkBaseDataAccessor<List<Task>> tasksDataAccessor;

  private final String offlineJobsPath;

  private final HelixAdapterSerializer adapter;

  private final RoutingDataRepository routingDataRepository;

  //TODO Add the serializer for near-line job later.
  public HelixJobRepository(@NotNull ZkClient zkClient, @NotNull HelixAdapterSerializer adapter,
      @NotNull String clusterName, RoutingDataRepository routingDataRepository) {
    this(zkClient, adapter, clusterName, routingDataRepository, new OfflineJobJSONSerializer(),
        new TasksJSONSerializer());
  }

  public HelixJobRepository(@NotNull ZkClient zkClient, @NotNull HelixAdapterSerializer adapter,
      @NotNull String clusterName, RoutingDataRepository routingDataRepository,
      VeniceSerializer<OfflineJob> jobSerializer, VeniceSerializer<List<Task>> taskVeniceSerializer) {
    this.routingDataRepository = routingDataRepository;
    offlineJobsPath = "/" + clusterName + "/OfflineJobs";
    this.adapter = adapter;
    this.adapter.registerSerializer(offlineJobsPath, jobSerializer);
    this.adapter.registerSerializer(offlineJobsPath + "/", taskVeniceSerializer);
    zkClient.setZkSerializer(this.adapter);
    jobDataAccessor = new ZkBaseDataAccessor<>(zkClient);
    tasksDataAccessor = new ZkBaseDataAccessor<>(zkClient);
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
      ExecutionStatus oldStatus = job.getStatus();
      job.setStatus(ExecutionStatus.ARCHIVED);
      try {
        updateJobToZK(job);
      } catch (Throwable e) {
        String errorMessage = "Can not update job:" + job.getJobId() + " to ZK.";
        logger.info(errorMessage, e);
        job.setStatus(oldStatus);
        throw new VeniceException(errorMessage, e);
      }
      this.jobMap.remove(jobId);
    } else {
      throw new VeniceException("Job:" + jobId + " is in " + job.getStatus() + " status. Can not be archived.");
    }
  }

  @Override
  public synchronized void updateTaskStatus(long jobId, @NotNull Task task) {
    Job job = this.getJob(jobId);
    Task oldTask = job.getTask(task.getPartitionId(), task.getInstanceId());
    job.updateTaskStatus(task);
    //Write updates to ZK at first.
    try {
      updateTaskToZK(job.getJobId(), task.getPartitionId(), job.tasksInPartition(task.getPartitionId()));
    } catch (Throwable e) {
      String errorMessage = "Can not update task:" + task.getTaskId() + ". Rollback local copy.";
      logger.info(errorMessage, e);
      //If met any error when updating task to ZK, rollback local copy.
      if (oldTask == null) {
        job.deleteTask(task);
      } else {
        job.setTask(oldTask);
      }
      throw new VeniceException(errorMessage, e);
    }
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
    routingDataRepository.unSubscribeRoutingDataChange(job.getKafkaTopic(), this);
    if (job.getStatus().equals(ExecutionStatus.STARTED)) {
      ExecutionStatus oldStaus = job.getStatus();
      job.setStatus(status);
      try {
        updateJobToZK(job);
      } catch (Throwable e) {
        String errorMessage = "Can not update job:" + job.getJobId() + " to ZK. Rollback the local copy.";
        logger.info(errorMessage, e);
        job.setStatus(oldStaus);
        throw new VeniceException(errorMessage, e);
      }
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
    job.updateExecutingParitions(routingDataRepository.getPartitions(job.getKafkaTopic()));
    job.setStatus(ExecutionStatus.STARTED);
    try {
      updateJobToZK(job);
      for (int partitionId = 0; partitionId < job.getNumberOfPartition(); partitionId++) {
        updateTaskToZK(job.getJobId(), partitionId, new ArrayList<>());
      }
    } catch (Throwable e) {
      String errorMessage = "Can not update job:" + job.getJobId() + " to ZK.";
      logger.info(errorMessage, e);
      throw new VeniceException(errorMessage, e);
    }
    this.jobMap.put(job.getJobId(), job);
    List<Job> jobs = this.topicToJobsMap.get(job.getKafkaTopic());
    if (jobs == null) {
      jobs = new ArrayList<>();
      topicToJobsMap.put(job.getKafkaTopic(), jobs);
    }
    jobs.add(job);
    routingDataRepository.subscribeRoutingDataChange(job.getKafkaTopic(), this);
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

  private void updateJobToZK(Job job) {
    if (job instanceof OfflineJob) {
      jobDataAccessor.set(offlineJobsPath + "/" + job.getJobId(), (OfflineJob) job, AccessOption.PERSISTENT);
    } else {
      throw new VeniceException("Only offline job is supported now.");
    }
  }

  private void updateTaskToZK(long jobId, int partitionId, List<Task> tasks) {
    tasksDataAccessor.set(offlineJobsPath + "/" + jobId + "/" + partitionId, tasks, AccessOption.PERSISTENT);
  }

  public synchronized void start() {
    jobMap = new HashMap<>();
    topicToJobsMap = new HashMap<>();
    logger.info("Start getting offline jobs from ZK");
    // We don't need to listen the change of jobs and tasks. The master controller is the only entrance to read/write
    // these data. When master is failed, another controller will take over this mastership and load from ZK when
    // becoming master.
    List<OfflineJob> offLineJobs = jobDataAccessor.getChildren(offlineJobsPath, null, AccessOption.PERSISTENT);
    logger.info("Get " + offLineJobs.size() + " offline jobs.");
    for (OfflineJob job : offLineJobs) {
      if (job.getStatus().equals(ExecutionStatus.ARCHIVED)) {
        //Archived job, do not add it to repository.
        continue;
      }
      jobMap.put(job.getJobId(), job);

      if (job.getStatus().equals(ExecutionStatus.COMPLETED) || job.getStatus().equals(ExecutionStatus.ERROR)) {
        //Only add running job to topicToJobsMap.
        continue;
      }
      List<Job> jobs = this.topicToJobsMap.get(job.getKafkaTopic());
      if (jobs == null) {
        jobs = new ArrayList<>();
        topicToJobsMap.put(job.getKafkaTopic(), jobs);
      }
      jobs.add(job);

      logger.info("Start getting tasks for job:" + job.getJobId());
      List<List<Task>> tasks =
          tasksDataAccessor.getChildren(offlineJobsPath + "/" + job.getJobId(), null, AccessOption.PERSISTENT);
      for (List<Task> task : tasks) {
        task.forEach(job::setTask);
      }
      updateJobPartitions(job, routingDataRepository.getPartitions(job.getKafkaTopic()));
      routingDataRepository.subscribeRoutingDataChange(job.getKafkaTopic(), this);
      logger.info("Filled tasks into job:" + job.getJobId());
      ExecutionStatus jobStatus = job.checkJobStatus();
      if (jobStatus.equals(ExecutionStatus.COMPLETED)) {
        stopJob(job.getJobId());
      } else if (jobStatus.equals(ExecutionStatus.ERROR)) {
        stopJobWithError(job.getJobId());
      }
    }
    logger.info("End getting offline jobs from zk");
  }

  public synchronized void clear() {
    this.jobMap.clear();
    this.topicToJobsMap.clear();
    this.adapter.unregisterSeralizer(offlineJobsPath);
    this.adapter.unregisterSeralizer(offlineJobsPath + "/");
    //We don't need to close ZK client here. It's could be reused by other repository.
  }

  private void updateJobPartitions(Job job, Map<Integer, Partition> partitions) {
    Set<Integer> changedPartitions = job.updateExecutingParitions(partitions);
    if (!changedPartitions.isEmpty()) {
      for (Integer partitionId : changedPartitions) {
        try {
          this.updateTaskToZK(job.getJobId(), partitionId, job.tasksInPartition(partitionId));
        } catch (Throwable e) {
          // We don't need to break the whole update process here. Because even if the local copy is different from ZK,
          // it will be sync up again when task status is changed. The worst case is before next task status update
          // happening, controller is failed. But new controller will read job and tasks from zk and check the
          // partitions again which will also fix this problem.
          logger.error("Can not update tasks to ZK for job:" + job.getJobId() + " in partition:" + partitionId);
        }
      }
    }
  }

  @Override
  public synchronized void handleRoutingDataChange(String kafkaTopic, Map<Integer, Partition> partitions) {
    List<Job> jobs = this.getRunningJobOfTopic(kafkaTopic);
    if (jobs.size() > 1) {
      throw new VeniceException(
          "There should be only one job running for each kafka topic. But now there are:" + jobs.size()
              + " jobs running.");
    }
    Job job = jobs.get(0);
    updateJobPartitions(job, partitions);
  }

  public RoutingDataRepository getRoutingDataRepository() {
    return routingDataRepository;
  }
}
