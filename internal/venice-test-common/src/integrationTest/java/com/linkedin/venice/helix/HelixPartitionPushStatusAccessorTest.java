package com.linkedin.venice.helix;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushmonitor.HybridStoreQuotaStatus;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.util.HashMap;
import java.util.Map;
import org.apache.helix.HelixAdmin;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.LeaderStandbySMD;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class HelixPartitionPushStatusAccessorTest {
  private String clusterName = "UnitTestCLuster";
  private HelixPartitionStatusAccessor accessor1, accessor2;
  private String topic = "testTopic";

  // Test behavior configuration
  private static final int WAIT_TIME = 1000; // FIXME: Non-deterministic. Will lead to flaky tests.

  private SafeHelixManager manager1, manager2;
  private HelixAdmin admin;
  private String resourceName = "UnitTest";
  private String zkAddress;
  private int httpPort1, httpPort2;
  private ZkServerWrapper zkServerWrapper;

  @BeforeMethod(alwaysRun = true)
  public void setupHelix() throws Exception {
    zkServerWrapper = ServiceFactory.getZkServer();
    zkAddress = zkServerWrapper.getAddress();
    admin = new ZKHelixAdmin(zkAddress);
    admin.addCluster(clusterName);
    HelixConfigScope configScope =
        new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER).forCluster(clusterName).build();
    Map<String, String> helixClusterProperties = new HashMap<String, String>();
    helixClusterProperties.put(ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN, String.valueOf(true));
    admin.setConfig(configScope, helixClusterProperties);
    admin.addStateModelDef(clusterName, LeaderStandbySMD.name, LeaderStandbySMD.build());

    admin.addResource(
        clusterName,
        resourceName,
        2,
        LeaderStandbySMD.name,
        IdealState.RebalanceMode.FULL_AUTO.toString());
    admin.rebalance(clusterName, resourceName, 1);

    // make sure httpPort1 and httpPort2 are different so that we have two different instances
    httpPort1 = 50000 + (int) (System.currentTimeMillis() % 10000);
    manager1 = TestUtils.getParticipant(
        clusterName,
        Utils.getHelixNodeIdentifier(Utils.getHostName(), httpPort1),
        zkAddress,
        httpPort1,
        LeaderStandbySMD.name);
    manager1.connect();
    // Waiting essential notification from ZK. TODO: use a listener to find out when ZK is ready
    Thread.sleep(WAIT_TIME);
    accessor1 = new HelixPartitionStatusAccessor(manager1.getOriginalManager(), manager1.getInstanceName(), false);

    httpPort2 = 50000 + (int) (System.currentTimeMillis() % 10000) + 1;
    manager2 = TestUtils.getParticipant(
        clusterName,
        Utils.getHelixNodeIdentifier(Utils.getHostName(), httpPort2),
        zkAddress,
        httpPort2,
        LeaderStandbySMD.name);
    manager2.connect();
    // Waiting essential notification from ZK. TODO: use a listener to find out when ZK is ready
    Thread.sleep(WAIT_TIME);
    accessor2 = new HelixPartitionStatusAccessor(manager2.getOriginalManager(), manager2.getInstanceName(), true);
  }

  @AfterMethod(alwaysRun = true)
  public void cleanupHelix() {
    manager1.disconnect();
    manager2.disconnect();
    admin.dropCluster(clusterName);
    admin.close();
    zkServerWrapper.close();
  }

  @Test
  public void testGetNonExistTopicReplicaStatus() {
    try {
      accessor1.getAllReplicaStatus(topic);
      Assert.fail("A venice exception should be thrown when getting a nonexist topic in ZK");
    } catch (VeniceException e) {
    }
  }

  @Test
  public void testGetNonExistReplicaStatus() {
    int partitionId = 0;
    try {
      accessor1.getReplicaStatus(topic, partitionId);
      Assert.fail("A venice exception should be thrown when getting a nonexist replica status in ZK");
    } catch (VeniceException e) {
    }
  }

  @Test
  public void testUpdateAndGetReplicaStatus() {
    int partitionId = 0;
    accessor1.updateReplicaStatus(topic, partitionId, ExecutionStatus.COMPLETED);
    accessor1.updateReplicaStatus(topic, partitionId, ExecutionStatus.PROGRESS);
    Assert.assertEquals(ExecutionStatus.PROGRESS, accessor1.getReplicaStatus(topic, partitionId));
  }

  @Test
  public void testMultipleUpdateAndGetReplicaStatus() {
    int partitionId0 = 0;
    int partitionId1 = 1;
    accessor1.updateReplicaStatus(topic, partitionId0, ExecutionStatus.COMPLETED);
    accessor1.updateReplicaStatus(topic, partitionId1, ExecutionStatus.PROGRESS);
    Assert.assertEquals(ExecutionStatus.COMPLETED, accessor1.getReplicaStatus(topic, partitionId0));
    Assert.assertEquals(ExecutionStatus.PROGRESS, accessor1.getReplicaStatus(topic, partitionId1));
    Map<Integer, ExecutionStatus> result = new HashMap<>();
    result.put(0, ExecutionStatus.COMPLETED);
    result.put(1, ExecutionStatus.PROGRESS);
    Assert.assertEquals(accessor1.getAllReplicaStatus(topic), result);
  }

  @Test
  public void testUpdateSamePartition() {
    int partitionId = 0;
    accessor1.updateReplicaStatus(topic, partitionId, ExecutionStatus.COMPLETED);
    Assert.assertEquals(ExecutionStatus.COMPLETED, accessor1.getReplicaStatus(topic, partitionId));

    accessor2.updateReplicaStatus(topic, partitionId, ExecutionStatus.PROGRESS);
    Assert.assertEquals(ExecutionStatus.PROGRESS, accessor2.getReplicaStatus(topic, partitionId));
  }

  @Test
  public void testUpdateDifferentPartitions() {
    int partitionId0 = 0;
    int partitionId1 = 1;
    accessor1.updateReplicaStatus(topic, partitionId0, ExecutionStatus.COMPLETED);
    accessor2.updateReplicaStatus(topic, partitionId1, ExecutionStatus.PROGRESS);

    Assert.assertEquals(ExecutionStatus.COMPLETED, accessor1.getReplicaStatus(topic, partitionId0));
    Assert.assertEquals(ExecutionStatus.PROGRESS, accessor2.getReplicaStatus(topic, partitionId1));
  }

  @Test
  public void testUpdateQuotaViolated() {
    int partitionId = 0;
    // But accessor0 can not report hybrid quota status, since helix hybrid store quota is not enabled.
    accessor1.updateHybridQuotaReplicaStatus(topic, partitionId, HybridStoreQuotaStatus.QUOTA_VIOLATED);
    Assert.assertEquals(HybridStoreQuotaStatus.UNKNOWN, accessor1.getHybridQuotaReplicaStatus(topic, partitionId));

    accessor2.updateHybridQuotaReplicaStatus(topic, partitionId, HybridStoreQuotaStatus.QUOTA_VIOLATED);
    Assert
        .assertEquals(HybridStoreQuotaStatus.QUOTA_VIOLATED, accessor2.getHybridQuotaReplicaStatus(topic, partitionId));
  }
}
