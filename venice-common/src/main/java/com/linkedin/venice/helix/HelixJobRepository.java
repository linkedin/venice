package com.linkedin.venice.helix;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.job.ExecutionStatus;
import com.linkedin.venice.job.Job;
import com.linkedin.venice.job.JobRepository;
import com.linkedin.venice.job.OfflineJob;
import com.linkedin.venice.job.Task;
import com.linkedin.venice.listener.ListenerManager;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.VeniceSerializer;
import com.linkedin.venice.utils.HelixUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import javax.validation.constraints.NotNull;

import com.linkedin.venice.utils.PathResourceRegistry;
import org.apache.helix.AccessOption;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.log4j.Logger;


/**
 * Job Repository which persist job data on Helix. For each job, there is one znode in path /OfflineJobs/$jobId. And for
 * tasks in the job, they will be grouped by partition and stored in znode /OfflineJobs/$jobId/$partitionId
 */
public class HelixJobRepository implements JobRepository {
  private static final Logger logger = Logger.getLogger(HelixJobRepository.class);
  /**
   * All of access to this map are protected by synchronized block. The repository is thread-safe.
   */
  private Map<String, List<Job>> topicToRunningJobsMap = new HashMap<>();

  private Map<String, List<Job>> topicToTerminatedJobsMap = new HashMap<>();

  private final ZkBaseDataAccessor<OfflineJob> jobDataAccessor;

  private final ZkBaseDataAccessor<List<Task>> tasksDataAccessor;

  private final String offlineJobsPath;

  // Patterned path for all jobs
  private final String offlineJobPathPattern;

  // Patterned path for all tasks
  private final String offlineTaskPathPattern;

  private final HelixAdapterSerializer adapter;

  private VeniceSerializer<OfflineJob> jobSerializer;

  private VeniceSerializer<List<Task>> taskVeniceSerializer;

  //TODO get retry count from configuration.
  private int retryCount = 3;

  public static final String OFFLINE_JOBS_SUB_PATH = "/OfflineJobs";

  private ListenerManager<JobStatusChangedListener> listenerManager;

  //TODO Add the serializer for near-line job later.
  public HelixJobRepository(@NotNull ZkClient zkClient, @NotNull HelixAdapterSerializer adapter,
      @NotNull String clusterName) {
    this(zkClient, adapter, clusterName, new OfflineJobJSONSerializer(), new TasksJSONSerializer());
  }

  public HelixJobRepository(@NotNull ZkClient zkClient, @NotNull HelixAdapterSerializer adapter,
      @NotNull String clusterName, VeniceSerializer<OfflineJob> jobSerializer,
      VeniceSerializer<List<Task>> taskVeniceSerializer) {
    this.offlineJobsPath = HelixUtils.getHelixClusterZkPath(clusterName) + OFFLINE_JOBS_SUB_PATH;
    this.offlineJobPathPattern = this.offlineJobsPath + "/" + PathResourceRegistry.WILDCARD_MATCH_ANY;
    this.offlineTaskPathPattern = this.offlineJobPathPattern + "/" + PathResourceRegistry.WILDCARD_MATCH_ANY;
    this.adapter = adapter;
    this.jobSerializer = jobSerializer;
    this.taskVeniceSerializer = taskVeniceSerializer;
    zkClient.setZkSerializer(this.adapter);
    this.jobDataAccessor = new ZkBaseDataAccessor<>(zkClient);
    this.tasksDataAccessor = new ZkBaseDataAccessor<>(zkClient);
    this.listenerManager = new ListenerManager<>();
  }

  @Override
  public synchronized List<Job> getRunningJobOfTopic(@NotNull String kafkaTopic) {
    List<Job> jobs = topicToRunningJobsMap.get(kafkaTopic);

    if (jobs == null) {
      jobs = Collections.emptyList();
    }
    return jobs;
  }

  @Override
  public synchronized List<Job> getAllRunningJobs() {
    List<Job> jobs = new ArrayList<>();
    topicToRunningJobsMap.values().forEach(jobs::addAll);
    return jobs;
  }

  @Override
  public synchronized List<Job> getTerminatedJobOfTopic(String kafkaTopic) {
    List<Job> jobs = topicToTerminatedJobsMap.get(kafkaTopic);
    if (jobs == null) {
      jobs = Collections.emptyList();
    }
    return jobs;
  }

  @Override
  public synchronized void archiveJob(long jobId, String kafkaTopic) {
    Job job = this.getJob(jobId, kafkaTopic);
    //TODO we should put archived job to some code/historic storage at first then delete it from zk.
    //updateJobStatus(job, ExecutionStatus.ARCHIVED);
    HelixUtils.remove(jobDataAccessor, getJobZKPath(job), retryCount);
    deleteJobFromMap(kafkaTopic, jobId, topicToTerminatedJobsMap);
    triggerJobStatusChangeEvent(job.cloneJob());
  }

  @Override
  public synchronized void updateTaskStatus(long jobId, String kafkaTopic, @NotNull Task task) {
    Job job = this.getJob(jobId, kafkaTopic);
    // Clone job and update status.
    Job clonedJob = job.cloneJob();
    clonedJob.updateTaskStatus(task);
    // Udate to ZK
    String path = getJobZKPath(clonedJob) + "/" + task.getPartitionId();
    HelixUtils.update(tasksDataAccessor, path, clonedJob.tasksInPartition(task.getPartitionId()), retryCount);
    // Update local copy at last.
    job.updateTaskStatus(task);
  }

  @Override
  public synchronized void updateJobExecutingTasks(long jobId, PartitionAssignment partitionAssignment) {
    Job job = getJob(jobId, partitionAssignment.getTopic());
    if (job.isTerminated()) {
      logger.warn("Job:" + jobId + " is terminated before, can not be updated again.");
      return;
    }
    // Clone job and verify the updates.
    Job cloneJob = job.cloneJob();
    Set<Integer> changedPartitions = cloneJob.updateExecutingTasks(partitionAssignment);
    // update zk
    if (!changedPartitions.isEmpty()) {
      try {
        updateTasksForPartitions(cloneJob, changedPartitions);
      } catch (VeniceException e) {
        // We don't need to break the whole update process here. Because even if the local copy is different from ZK,
        // it will be sync up again when task status is changed. The worst case is before next task status update
        // happening, controller is failed. But new controller will read job and tasks from zk and check the
        // partitions again which will also fix this problem.
        logger.error(
            "Can not update tasks to ZK for job:" + job.getJobId() + " for kafka topic:+" + job.getKafkaTopic());
      }
    }
    // Update local copy.
    job.updateExecutingTasks(partitionAssignment);
  }

  @Override
  public synchronized void stopJob(long jobId, String kafkaTopic) {
    internalStopJob(jobId, kafkaTopic, ExecutionStatus.COMPLETED);
  }

  // TODO stopping a job, does not clean up the resources on storage node.
  // As Helix resource is not updated to Error/offline.
  @Override
  public synchronized void stopJobWithError(long jobId, String kafkaTopic) {
    internalStopJob(jobId, kafkaTopic, ExecutionStatus.ERROR);
  }

  private void internalStopJob(long jobId, String kafkaTopic, ExecutionStatus status) {
    Job job = this.getJob(jobId, kafkaTopic);
    updateJobStatus(job, status);
    //Remove from running jobs and add it to terminated jobs.
    deleteJobFromMap(kafkaTopic, job.getJobId(), topicToRunningJobsMap);
    addJobToMap(kafkaTopic, job, topicToTerminatedJobsMap);
    logger.info("Terminated job:" + jobId + " for kafka topic:" + kafkaTopic);
    triggerJobStatusChangeEvent(job.cloneJob());
  }

  @Override
  public synchronized void startJob(@NotNull Job job) {
    // Update job to ZK and local copy.
    updateJobStatus(job, ExecutionStatus.STARTED);
    //Update tasks of all partitions to ZK.
    Set<Integer> partitions = new HashSet<>();
    for (int i = 0; i < job.getNumberOfPartition(); i++) {
      partitions.add(i);
    }
    updateTasksForPartitions(job, partitions);
    // Add job in to running job map.
    addJobToMap(job.getKafkaTopic(), job, topicToRunningJobsMap);
    triggerJobStatusChangeEvent(job.cloneJob());
  }

  @Override
  public synchronized ExecutionStatus getJobStatus(long jobId, String kafkaTopic) {
    Job job = this.getJob(jobId, kafkaTopic);
    return job.getStatus();
  }

  @Override
  public synchronized Job getJob(long jobId, String kafkaTopic) {
    Job job = getJobFromMap(kafkaTopic, jobId, topicToRunningJobsMap);
    if (job != null) {
      return job;
    }
    job = getJobFromMap(kafkaTopic, jobId, topicToTerminatedJobsMap);
    if (job != null) {
      return job;
    }
    throw new VeniceException("Job for kafka topic:" + kafkaTopic + " does not exist.");
  }

  @Override
  public void subscribeJobStatusChange(String kafkaTopic, JobStatusChangedListener listener) {
    listenerManager.subscribe(kafkaTopic, listener);
  }

  @Override
  public void unsubscribeJobStatusChange(String kafkaTopic, JobStatusChangedListener listener) {
    listenerManager.unsubscribe(kafkaTopic, listener);
  }

  private void updateJobStatus(Job job, ExecutionStatus status) {
    // Validate the status at first.
    job.validateJobStatusTransition(status);
    // Clone job and update status.
    Job clonedJob = job.cloneJob();
    clonedJob.setStatus(status);
    // Update clone job to ZK.
    HelixUtils.update(jobDataAccessor, getJobZKPath(clonedJob), (OfflineJob) clonedJob, retryCount);
    // Update local copy at last.
    job.setStatus(status);
  }

  // TODO Helix bulk update API is not the atomic operation. When some of znode is failed to be updated, local copy and
  // TODO zk is inconsistent. Should test and handle this case in the further.
  private void updateTasksForPartitions(Job job, Set<Integer> partitions) {
    List<String> paths = new ArrayList<>();
    List<List<Task>> values = new ArrayList<>();
    for (Integer partition : partitions) {
      paths.add(getJobZKPath(job) + "/" + partition);
      List<Task> tasks = job.tasksInPartition(partition);
      values.add(tasks);
    }
    HelixUtils.updateChildren(tasksDataAccessor,paths,values,retryCount);
  }

  public void refresh() {
    clear();
    // TODO: Considering serializer should be thread-safe, we can share the serializer across multiple
    // clusters, which means we can register the following paths:
    // job serializer: /*/OfflineJobs/*
    // task serializer: /*/OfflineJobs/*/*
    this.adapter.registerSerializer(offlineJobPathPattern, jobSerializer);
    this.adapter.registerSerializer(offlineTaskPathPattern, taskVeniceSerializer);
    synchronized (this) {
      logger.info("Start getting offline jobs from ZK");
      // We don't need to listen the change of jobs and tasks. The master controller is the only entrance to read/write
      // these data. When master is failed, another controller will take over this mastership and load from ZK when
      // becoming master.
      List<OfflineJob> offLineJobs = jobDataAccessor.getChildren(offlineJobsPath, null, AccessOption.PERSISTENT);
      logger.info("Get " + offLineJobs.size() + " offline jobs.");
      for (OfflineJob job : offLineJobs) {
        switch (job.getStatus()) {
          case ERROR:
          case COMPLETED:
            addJobToMap(job.getKafkaTopic(), job, topicToTerminatedJobsMap);
            loadTasksForJob(job);
            break;
          case STARTED:
            addJobToMap(job.getKafkaTopic(), job, topicToRunningJobsMap);
            loadTasksForJob(job);
            break;
          case ARCHIVED:
            // remove archived job from zk.
            HelixUtils.remove(jobDataAccessor, getJobZKPath(job), retryCount);
            //TODO add archived job to some code/historic storage.
            break;
          case NEW:
            // We never put NEW status job to ZK. After job is created, its status is NEW by default. Then job manager
            // will waiting for enough executor being assigned. After that job will become STARTED and be updated to ZK.
            // If controller is failed during waiting, new controller can not recover NEW job from ZK. At last H2V query
            // the status of this job and get exception. The version for this failed job should be collected later by
            // schedule service.
          default:
            logger.error("Invalid job status:" + job.getStatus() + " for job:" + job.getJobId());
        }
      }
    }
    logger.info("End getting offline jobs from zk");
  }

  private void loadTasksForJob(Job job){
    logger.info("Start getting tasks for job:" + job.getJobId());
    List<List<Task>> tasks = tasksDataAccessor.getChildren(getJobZKPath(job), null, AccessOption.PERSISTENT);
    for (List<Task> task : tasks) {
      task.forEach(job::addTask);
    }
    logger.info("Filled tasks into job:" + job.getJobId());
  }

  private String getJobZKPath(Job job) {
    if (job instanceof OfflineJob) {
      return offlineJobsPath + "/" + job.getJobId();
    }
    throw new VeniceException("Only offline job is supported right now.");
  }

  private void addJobToMap(String kafkaTopic, Job job, Map<String, List<Job>> map) {
    List<Job> jobs = map.get(kafkaTopic);
    if (jobs == null) {
      jobs = new ArrayList<>();
      map.put(kafkaTopic, jobs);
    }
    jobs.add(job);
  }

  private void deleteJobFromMap(String kafkaTopic, long jobId, Map<String, List<Job>> map) {
    List<Job> jobs = map.get(kafkaTopic);
    if (jobs == null) {
      throw new VeniceException("Can not find kafka topic:" + kafkaTopic + " when deleting job:" + jobId);
    }
    boolean isFound = false;
    for (int i = 0; i < jobs.size(); i++) {
      if (jobs.get(i).getJobId() == jobId) {
        jobs.remove(i);
        isFound = true;
        break;
      }
    }
    if (!isFound) {
      throw new VeniceException("Can not find job:" + jobId);
    } else {
      if (jobs.isEmpty()) {
        map.remove(kafkaTopic);
      }
    }
  }

  private Job getJobFromMap(String kafkaTopic, long jobId, Map<String, List<Job>> map) {
    List<Job> jobs = map.get(kafkaTopic);
    if (jobs == null) {
      return null;
    }
    for (int i = 0; i < jobs.size(); i++) {
      if (jobs.get(i).getJobId() == jobId) {
        return jobs.get(i);
      }
    }
    return null;
  }

  private void triggerJobStatusChangeEvent(Job job) {
    listenerManager.trigger(job.getKafkaTopic(), new Function<JobStatusChangedListener, Void>() {
      @Override
      public Void apply(JobStatusChangedListener listener) {
        listener.onJobStatusChange(job);
        return null;
      }
    });
  }

  public synchronized void clear() {
    this.topicToRunningJobsMap.clear();
    this.topicToTerminatedJobsMap.clear();
    this.adapter.unregisterSeralizer(offlineJobPathPattern);
    this.adapter.unregisterSeralizer(offlineTaskPathPattern);
    //We don't need to close ZK client here. It's could be reused by other repository.
  }

}
