package com.linkedin.venice.helix;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.job.ExecutionStatus;
import com.linkedin.venice.job.OfflineJob;
import com.linkedin.venice.job.Task;
import com.linkedin.venice.job.WaitAllJobStatsDecider;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.utils.MockTestStateModel;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.LiveInstanceInfoProvider;
import org.apache.helix.ZNRecord;
import org.apache.helix.controller.HelixControllerMain;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Test cases for HelixJobRepository
 */
public class TestHelixJobRepository {
  private static final Logger LOG = Logger.getLogger(TestHelixJobRepository.class);

  private String zkAddress;
  private ZkClient zkClient;
  private String cluster; // = "test-job-cluster";
  private String clusterPath; // = "/test-job-cluster";
  private String jobPath = "/jobs";
  private HelixAdapterSerializer adapter = new HelixAdapterSerializer();
  private HelixJobRepository repository;
  private HelixRoutingDataRepository routingDataRepository;
  private HelixAdmin admin;
  private HelixManager controller;
  private HelixManager manager;
  private String kafkaTopic; // = "test_resource_1";
  private String nodeId = "localhost_9985";
  private ZkServerWrapper zkServerWrapper;

  @BeforeMethod
  public void setup()
      throws Exception {
    zkServerWrapper = ServiceFactory.getZkServer();
    zkAddress = zkServerWrapper.getAddress();

    kafkaTopic = TestUtils.getUniqueString("test_resource");
    cluster = TestUtils.getUniqueString("test-job-cluster");
    clusterPath = "/" + cluster;

    admin = new ZKHelixAdmin(zkAddress);
    admin.addCluster(cluster);
    HelixConfigScope configScope = new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER).
        forCluster(cluster).build();
    Map<String, String> helixClusterProperties = new HashMap<String, String>();
    helixClusterProperties.put(ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN, String.valueOf(true));
    admin.setConfig(configScope, helixClusterProperties);
    admin.addStateModelDef(cluster, MockTestStateModel.UNIT_TEST_STATE_MODEL,
        MockTestStateModel.getDefinition());

    controller = HelixControllerMain
        .startHelixController(zkAddress, cluster, Utils.getHelixNodeIdentifier(12345), HelixControllerMain.STANDALONE);
    manager = TestUtils.getParticipant(cluster, nodeId, zkAddress, 9986,
        MockTestStateModel.UNIT_TEST_STATE_MODEL);
    manager.connect();

    routingDataRepository = new HelixRoutingDataRepository(controller);
    routingDataRepository.refresh();

    zkClient = new ZkClient(zkAddress, ZkClient.DEFAULT_SESSION_TIMEOUT, ZkClient.DEFAULT_CONNECTION_TIMEOUT, adapter);
    //zkClient.create(clusterPath, null, CreateMode.PERSISTENT);
    zkClient.create(clusterPath + jobPath, null, CreateMode.PERSISTENT);
    repository = new HelixJobRepository(zkClient, adapter, cluster);
    repository.refresh();

    admin.addResource(cluster, kafkaTopic, 1, MockTestStateModel.UNIT_TEST_STATE_MODEL,
        IdealState.RebalanceMode.FULL_AUTO.toString());
    admin.rebalance(cluster, kafkaTopic, 1);
  }

  @AfterMethod
  public void cleanup() {
    repository.clear();
    routingDataRepository.clear();
    manager.disconnect();
    controller.disconnect();
    admin.dropCluster(cluster);
    admin.close();
    zkClient.deleteRecursive(clusterPath);
    zkClient.close();
    zkServerWrapper.close();
  }

  @Test
  public void testStartJob() {
    OfflineJob job = new OfflineJob(1, kafkaTopic, 1, 1);
    repository.startJob(job);
    Assert.assertEquals(job, repository.getJob(1, kafkaTopic),
        "Can not get correct job from repository after starting the job.");
    Assert.assertEquals(ExecutionStatus.STARTED, repository.getJobStatus(1, kafkaTopic),
        "Job should be running after being started.");
  }

  @Test
  public void testStartMultiPartitionsJob()
      throws Exception {
    String resource = TestUtils.getUniqueString("multi_test");
    int partitions = 5;
    HelixManager[] managers = new HelixManager[3];
    for (int i = 0; i < 3; i++) {
      HelixManager manager = TestUtils.getParticipant(cluster, "localhost_" + i, zkAddress, 9986,
          MockTestStateModel.UNIT_TEST_STATE_MODEL);
      manager.connect();
      managers[i] = manager;
    }
    admin.addResource(cluster, resource, partitions,
        MockTestStateModel.UNIT_TEST_STATE_MODEL,
        IdealState.RebalanceMode.FULL_AUTO.toString());
    admin.rebalance(cluster, resource, 1);

    OfflineJob job = new OfflineJob(1, resource, partitions, 1);
    repository.startJob(job);

    Assert.assertEquals(job, repository.getJob(1, resource),
        "Can not get correct job from repository after starting the job.");
    Assert.assertEquals(ExecutionStatus.STARTED, repository.getJobStatus(1, resource),
        "Job should be running after being started.");

    admin.dropResource(cluster, resource);
    for (HelixManager manager : managers) {
      manager.disconnect();
    }
  }

  @Test
  public void testStopJob() {
    OfflineJob job = new OfflineJob(1, kafkaTopic, 1, 1);
    repository.startJob(job);

    repository.stopJob(1, kafkaTopic);
    Assert.assertEquals(job, repository.getJob(1, kafkaTopic),
        "Can not get correct job from repository after starting the job.");
    Assert.assertEquals(ExecutionStatus.COMPLETED, repository.getJobStatus(1, kafkaTopic),
        "Job should be completed after being stopped.");

    Assert.assertTrue(repository.getRunningJobOfTopic(kafkaTopic).isEmpty(),
        "Job is stooped, should not exist in running job list.");
    Assert.assertEquals(repository.getTerminatedJobOfTopic(kafkaTopic).size(), 1);
  }

  @Test
  public void testStopJobWithError() {
    OfflineJob job = new OfflineJob(1, kafkaTopic, 1, 1);
    repository.startJob(job);

    repository.stopJobWithError(1, kafkaTopic);
    Assert.assertEquals(job, repository.getJob(1, kafkaTopic),
        "Can not get correct job from repository after starting the job.");
    Assert.assertEquals(ExecutionStatus.ERROR, repository.getJobStatus(1, kafkaTopic),
        "Job should be completed after being stopped.");

    Assert.assertTrue(repository.getRunningJobOfTopic(kafkaTopic).isEmpty(),
        "Job is stooped, should not exist in running job list.");
    Assert.assertEquals(repository.getTerminatedJobOfTopic(kafkaTopic).size(), 1);
  }

  @Test
  public void testStopNotStartedJob() {
    OfflineJob job = new OfflineJob(1, kafkaTopic, 1, 1);
    try {
      repository.stopJob(1, kafkaTopic);
      Assert.fail("Job is not started.");
    } catch (VeniceException e) {
      //expected
    }
  }

  @Test
  public void testStopNotRunningJob() {
    OfflineJob job = new OfflineJob(1, kafkaTopic, 1, 1);
    repository.startJob(job);
    job.setStatus(ExecutionStatus.COMPLETED);
    try {
      repository.stopJob(1, kafkaTopic);
      Assert.fail("Job is not in running status.");
    } catch (VeniceException e) {
      //expected
    }
  }

  @Test
  public void testArchiveJob() {
    OfflineJob job = new OfflineJob(1, kafkaTopic, 1, 1);
    repository.startJob(job);
    repository.stopJob(1, kafkaTopic);
    repository.archiveJob(1, kafkaTopic);
    try {
      repository.getJob(1, kafkaTopic);
      Assert.fail("Job is archived, should not exist in repository right now.");
    } catch (VeniceException e) {
      //expected
    }
    Assert.assertTrue(repository.getRunningJobOfTopic(kafkaTopic).isEmpty(),
        "Job is archived, should not exist in repository right now.");
    Assert.assertTrue(repository.getTerminatedJobOfTopic(kafkaTopic).isEmpty(),
        "Job is archived, should not exist in repository right now.");
  }

  @Test
  public void testArchiveNotCompleteJob() {
    OfflineJob job = new OfflineJob(1, kafkaTopic, 1, 1);
    repository.startJob(job);
    try {
      repository.archiveJob(1, kafkaTopic);
      Assert.fail("Job is not terminated, can not be archived.");
    } catch (VeniceException e) {
      //expected
    }
  }

  @Test
  public void testGetJobForTopic() {
    OfflineJob job = new OfflineJob(1, kafkaTopic, 1, 1);
    repository.startJob(job);
    Assert.assertEquals(job.getJobId(), repository.getRunningJobOfTopic(kafkaTopic).get(0).getJobId(),
        "Can not find job when it's running.");

    repository.stopJobWithError(job.getJobId(), job.getKafkaTopic());
    Assert.assertEquals(job.getJobId(), repository.getTerminatedJobOfTopic(kafkaTopic).get(0).getJobId(),
        " Can not find job when it's terminated.");
  }

  @Test
  public void testLoadFromZKJobAlreadyRunning()
      throws Exception {
    int numberOfPartition = 3;
    int replicaFactor = 1;
    String topic = TestUtils.getUniqueString("runningjob");
    admin.addResource(cluster, topic, numberOfPartition,
        MockTestStateModel.UNIT_TEST_STATE_MODEL,
        IdealState.RebalanceMode.FULL_AUTO.toString());
    admin.rebalance(cluster, topic, replicaFactor);

    OfflineJob job = new OfflineJob(1, topic, numberOfPartition, replicaFactor);
    repository.startJob(job);
    routingDataRepository.clear();
    manager.disconnect();
    repository.clear();
    Thread.sleep(1000l);

    HelixJobRepository newRepository = new HelixJobRepository(zkClient, adapter, cluster);
    routingDataRepository.refresh();
    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        newRepository.refresh();
        Assert.assertEquals(newRepository.getJob(1, topic).getStatus(), ExecutionStatus.STARTED,
            "Can not get job status from ZK correctly");
      }
    });
    thread.start();
    Thread.sleep(1000l);
    //Mock up the scenario that controller refresh at first then participant refresh up.
    manager = TestUtils.getParticipant(cluster, nodeId, zkAddress, 9986,
        MockTestStateModel.UNIT_TEST_STATE_MODEL);
    manager.connect();

    newRepository.clear();
  }

  @Test
  public void testLoadFromZk()
      throws InterruptedException {
    int jobCount = 3;
    int numberOfPartition = 10;
    int replicaFactor = 1;
    for (int i = 0; i < jobCount; i++) {
      admin.addResource(cluster, "topic" + i, numberOfPartition,
          MockTestStateModel.UNIT_TEST_STATE_MODEL,
          IdealState.RebalanceMode.FULL_AUTO.toString());
      admin.rebalance(cluster, "topic" + i, 1);
    }

    // There is a non-deterministic wait-time while the routing data is updated.
    long totalWaitTime = 10 * Time.MS_PER_SECOND;
    long startTime = System.currentTimeMillis();
    OfflineJob[] jobs = new OfflineJob[jobCount];
    LOG.info("Starting the OfflineJob update process.");
    for (int i = 0; i < jobCount; i++) {
      String topicName = "topic" + i;
      final OfflineJob job = new OfflineJob(i, topicName, numberOfPartition, replicaFactor);
      repository.startJob(job);
      long elapsedTime = System.currentTimeMillis() - startTime;
      long timeOut = Math.max(1, totalWaitTime - elapsedTime);
      TestUtils.waitForNonDeterministicCompletion(timeOut, TimeUnit.MILLISECONDS, () -> {
        try {
          //Enough partition count and replica factor in each partition.
          PartitionAssignment partitionAssignment = routingDataRepository.getPartitionAssignments(topicName);
          if (partitionAssignment.getAssignedNumberOfPartitions() != numberOfPartition) {
            return false;
          }
          for (Partition p : partitionAssignment.getAllPartitions()) {
            if (p.getReadyToServeInstances().size() != replicaFactor) {
              return false;
            }
          }
          return true;
        } catch (VeniceException ve) {
          LOG.error("Got a VeniceException from job.updateExecutingTasks()", ve);
          return false;
        }
      });

      job.updateExecutingTasks(routingDataRepository.getPartitionAssignments(topicName));
      jobs[i] = job;
    }

    //Start one task for job0 and job1
    for (int i = 0; i < jobCount - 1; i++) {
      OfflineJob job = jobs[i];
      Task t1 = new Task(job.generateTaskId(3, nodeId), 3, nodeId, ExecutionStatus.STARTED);
      repository.updateTaskStatus(i, "topic" + i, t1);
    }
    //Failed one task for job1
    Task t2 = new Task(jobs[1].generateTaskId(3, nodeId), 3, nodeId, ExecutionStatus.ERROR);
    repository.updateTaskStatus(1, "topic1", t2);

    int completedProgress = 1000;
    for (int i = 0; i < numberOfPartition; i++) {
      for (int j = 0; j < replicaFactor; j++) {
        Task task = new Task(jobs[2].generateTaskId(i, nodeId), i, nodeId, ExecutionStatus.STARTED);
        repository.updateTaskStatus(2, "topic2", task);
        task = new Task(jobs[2].generateTaskId(i, nodeId), i, nodeId, ExecutionStatus.COMPLETED);
        task.setProgress(completedProgress);
        repository.updateTaskStatus(2, "topic2", task);
      }
    }
    HelixJobRepository newRepository = new HelixJobRepository(zkClient, adapter, cluster);
    newRepository.refresh();
    Assert.assertEquals(newRepository.getJob(0, "topic0").getTaskStatus(3, jobs[0].generateTaskId(3, nodeId)),
        ExecutionStatus.STARTED, "Task dose not be loaded correctly.");
    Assert.assertEquals(newRepository.getJob(1, "topic1").getTaskStatus(3, jobs[1].generateTaskId(3, nodeId)),
        ExecutionStatus.ERROR, "Task dose not be loaded correctly.");
    Assert.assertEquals(newRepository.getJob(2, "topic2").getTaskStatus(1, jobs[2].generateTaskId(1, nodeId)),
        ExecutionStatus.COMPLETED, "Task dose not be loaded correctly.");
    Assert.assertEquals(newRepository.getJob(2, "topic2").getTask(1, jobs[2].generateTaskId(1, nodeId)).getProgress(),
        completedProgress, "Task dose not be loaded correctly.");

    Assert.assertEquals(newRepository.getJobStatus(0, "topic0"), ExecutionStatus.STARTED,
        "Job should be started and updated to ZK");
    WaitAllJobStatsDecider decider = new WaitAllJobStatsDecider();
    Assert.assertEquals(decider.checkJobStatus(newRepository.getJob(1,"topic1")), ExecutionStatus.ERROR,
        "Job should be failed.");
    Assert.assertEquals(decider.checkJobStatus(newRepository.getJob(2, "topic2")), ExecutionStatus.COMPLETED,
        "Job should be completed");

    Assert.assertEquals(newRepository.getAllRunningJobs().size(), 3);
    Assert.assertEquals(newRepository.getRunningJobOfTopic("topic0").size(), 1);

    newRepository.clear();
  }

  @Test
  public void testUpdateTaskStatusFromMultiInstances(){
    // Start a job with 1 partition and 2 replicas
    OfflineJob job = new OfflineJob(1, kafkaTopic, 1, 2);
    String instanceId1 = "testUpdateTaskStatusInstance1";
    String instanceId2 = "testUpdateTaskStatusInstance2";
    String taskId1= job.generateTaskId(0, instanceId1);
    String taskId2= job.generateTaskId(0, instanceId2);
    Task task1 = new Task(taskId1, 0, instanceId1, ExecutionStatus.STARTED);
    Task task2 = new Task(taskId2, 0, instanceId2, ExecutionStatus.STARTED);
    job.addTask(task1);
    job.addTask(task2);
    repository.startJob(job);
    // Verify the tasks status are both STARTED
    Assert.assertEquals(repository.getRunningJobOfTopic(kafkaTopic).get(0).getTaskStatus(0, taskId1), ExecutionStatus.STARTED);
    Assert.assertEquals(repository.getRunningJobOfTopic(kafkaTopic).get(0).getTaskStatus(0, taskId2), ExecutionStatus.STARTED);
    // Update progress for tasks
    int task1Progress = 100;
    int task2Progress = 200;
    task1 = new Task(taskId1, 0, instanceId1, ExecutionStatus.PROGRESS);
    task1.setProgress(task1Progress);
    task2 = new Task(taskId2, 0, instanceId2, ExecutionStatus.PROGRESS);
    task2.setProgress(task2Progress);
    repository.updateTaskStatus(job.getJobId(), kafkaTopic, task1);
    repository.updateTaskStatus(job.getJobId(), kafkaTopic, task2);
    Assert.assertEquals(repository.getRunningJobOfTopic(kafkaTopic).get(0).getTask(0, taskId1).getProgress(), task1Progress);
    Assert.assertEquals(repository.getRunningJobOfTopic(kafkaTopic).get(0).getTask(0, taskId2).getProgress(), task2Progress);

    //Reload all of data from ZK
    repository.refresh();
    //Verify all of data has been updated to ZK correctly
    Assert.assertEquals(repository.getRunningJobOfTopic(kafkaTopic).get(0).getStatus(), ExecutionStatus.STARTED);
    Assert.assertEquals(repository.getRunningJobOfTopic(kafkaTopic).get(0).getTaskStatus(0, taskId1), ExecutionStatus.PROGRESS);
    Assert.assertEquals(repository.getRunningJobOfTopic(kafkaTopic).get(0).getTaskStatus(0, taskId2), ExecutionStatus.PROGRESS);
    Assert.assertEquals(repository.getRunningJobOfTopic(kafkaTopic).get(0).getTask(0, taskId1).getProgress(), task1Progress);
    Assert.assertEquals(repository.getRunningJobOfTopic(kafkaTopic).get(0).getTask(0, taskId2).getProgress(), task2Progress);
  }
}
