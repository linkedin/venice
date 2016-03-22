package com.linkedin.venice.helix;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.job.ExecutionStatus;
import com.linkedin.venice.job.OfflineJob;
import com.linkedin.venice.job.Task;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.utils.Utils;
import java.util.HashMap;
import java.util.Map;
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
import org.apache.zookeeper.CreateMode;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Test cases for HelixJobRepository
 */
public class TestHelixJobRepository {
  private String zkAddress;
  private ZkClient zkClient;
  private String cluster = "test-job-cluster";
  private String clusterPath = "/test-job-cluster";
  private String jobPath = "/jobs";
  private HelixAdapterSerializer adapter = new HelixAdapterSerializer();
  private HelixJobRepository repository;
  private HelixRoutingDataRepository routingDataRepository;
  private HelixAdmin admin;
  private HelixManager controller;
  private HelixManager manager;
  private String kafkaTopic = "test_resource_1";
  private String nodeId = "localhost_9985";
  private ZkServerWrapper zkServerWrapper;

  @BeforeMethod
  public void setup()
      throws Exception {
    zkServerWrapper = ServiceFactory.getZkServer();
    zkAddress = zkServerWrapper.getAddress();

    admin = new ZKHelixAdmin(zkAddress);
    admin.addCluster(cluster);
    HelixConfigScope configScope = new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER).
        forCluster(cluster).build();
    Map<String, String> helixClusterProperties = new HashMap<String, String>();
    helixClusterProperties.put(ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN, String.valueOf(true));
    admin.setConfig(configScope, helixClusterProperties);
    admin.addStateModelDef(cluster, TestHelixRoutingDataRepository.UnitTestStateModel.UNIT_TEST_STATE_MODEL,
        TestHelixRoutingDataRepository.UnitTestStateModel.getDefinition());

    controller = HelixControllerMain
        .startHelixController(zkAddress, cluster, "UnitTestController", HelixControllerMain.STANDALONE);
    manager = HelixManagerFactory.getZKHelixManager(cluster, nodeId, InstanceType.PARTICIPANT, zkAddress);
    manager.getStateMachineEngine()
        .registerStateModelFactory(TestHelixRoutingDataRepository.UnitTestStateModel.UNIT_TEST_STATE_MODEL,
            new TestHelixRoutingDataRepository.UnitTestStateModelFactory());
    Instance instance = new Instance(nodeId, Utils.getHostName(), 9986, 9985);
    manager.setLiveInstanceInfoProvider(new LiveInstanceInfoProvider() {
      @Override
      public ZNRecord getAdditionalLiveInstanceInfo() {
        return HelixInstanceConverter.convertInstanceToZNRecord(instance);
      }
    });
    manager.connect();

    routingDataRepository = new HelixRoutingDataRepository(controller);
    routingDataRepository.start();

    zkClient = new ZkClient(zkAddress, ZkClient.DEFAULT_SESSION_TIMEOUT, ZkClient.DEFAULT_CONNECTION_TIMEOUT, adapter);
    //zkClient.create(clusterPath, null, CreateMode.PERSISTENT);
    zkClient.create(clusterPath + jobPath, null, CreateMode.PERSISTENT);
    repository = new HelixJobRepository(zkClient, adapter, cluster, routingDataRepository);
    repository.start();

    admin.addResource(cluster, kafkaTopic, 1, TestHelixRoutingDataRepository.UnitTestStateModel.UNIT_TEST_STATE_MODEL,
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
    String resource = "multi_test";
    int partitions = 5;
    HelixManager[] managers = new HelixManager[3];
    for (int i = 0; i < 3; i++) {
      HelixManager manager =
          HelixManagerFactory.getZKHelixManager(cluster, "localhost_" + i, InstanceType.PARTICIPANT, zkAddress);
      manager.getStateMachineEngine()
          .registerStateModelFactory(TestHelixRoutingDataRepository.UnitTestStateModel.UNIT_TEST_STATE_MODEL,
              new TestHelixRoutingDataRepository.UnitTestStateModelFactory());
      Instance instance = new Instance("localhost_" + i, Utils.getHostName(), 9986, 9985);
      manager.setLiveInstanceInfoProvider(new LiveInstanceInfoProvider() {
        @Override
        public ZNRecord getAdditionalLiveInstanceInfo() {
          return HelixInstanceConverter.convertInstanceToZNRecord(instance);
        }
      });
      manager.connect();
      managers[i] = manager;
    }
    admin.addResource(cluster, resource, partitions,
        TestHelixRoutingDataRepository.UnitTestStateModel.UNIT_TEST_STATE_MODEL,
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

    try {
      repository.getRunningJobOfTopic("topic1");
      Assert.fail("Job is stooped, should not exist in running job list.");
    } catch (VeniceException e) {
      //expected
    }
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
    try {
      repository.getRunningJobOfTopic(kafkaTopic);
      Assert.fail("Job is archived, should not exist in repository right now.");
    } catch (VeniceException e) {
      //expected
    }
    try {
      repository.getTerminatedJobOfTopic(kafkaTopic);
      Assert.fail("Job is archived, should not exist in repository right now.");
    } catch (VeniceException e) {
      //expected
    }
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
  public void testLoadFromZk()
      throws InterruptedException {
    int jobCount = 3;
    int numberOfPartition = 10;
    int replicaFactor = 1;
    for (int i = 0; i < jobCount; i++) {
      admin.addResource(cluster, "topic" + i, numberOfPartition,
          TestHelixRoutingDataRepository.UnitTestStateModel.UNIT_TEST_STATE_MODEL,
          IdealState.RebalanceMode.FULL_AUTO.toString());
      admin.rebalance(cluster, "topic" + i, 1);
    }
    Thread.sleep(1000l);
    OfflineJob[] jobs = new OfflineJob[jobCount];
    for (int i = 0; i < jobCount; i++) {
      OfflineJob job = new OfflineJob(i, "topic" + i, numberOfPartition, replicaFactor);
      repository.startJob(job);
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

    for (int i = 0; i < numberOfPartition; i++) {
      for (int j = 0; j < replicaFactor; j++) {
        Task task = new Task(jobs[2].generateTaskId(i, nodeId), i, nodeId, ExecutionStatus.STARTED);
        repository.updateTaskStatus(2, "topic2", task);
        task = new Task(jobs[2].generateTaskId(i, nodeId), i, nodeId, ExecutionStatus.COMPLETED);
        repository.updateTaskStatus(2, "topic2", task);
      }
    }
    HelixJobRepository newRepository = new HelixJobRepository(zkClient, adapter, cluster, routingDataRepository);
    newRepository.start();
    Assert.assertEquals(newRepository.getJob(0, "topic0").getTaskStatus(3, jobs[0].generateTaskId(3, nodeId)),
        ExecutionStatus.STARTED, "Task dose not be loaded correctly.");
    Assert.assertEquals(newRepository.getJob(1, "topic1").getTaskStatus(3, jobs[1].generateTaskId(3, nodeId)),
        ExecutionStatus.ERROR, "Task dose not be loaded correctly.");
    Assert.assertEquals(newRepository.getJob(2, "topic2").getTaskStatus(1, jobs[2].generateTaskId(1, nodeId)),
        ExecutionStatus.COMPLETED, "Task dose not be loaded correctly.");

    Assert.assertEquals(newRepository.getJobStatus(0, "topic0"), ExecutionStatus.STARTED,
        "Job should be started and updated to ZK");
    Assert.assertEquals(newRepository.getJobStatus(1, "topic1"), ExecutionStatus.ERROR,
        "Job should be failed because one of task is failed.");
    Assert.assertEquals(newRepository.getJobStatus(2, "topic2"), ExecutionStatus.COMPLETED,
        "Job should be started and updated to ZK");
    newRepository.clear();
  }
}
