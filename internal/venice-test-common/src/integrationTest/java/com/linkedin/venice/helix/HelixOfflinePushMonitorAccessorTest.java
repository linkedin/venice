package com.linkedin.venice.helix;

import static com.linkedin.venice.helix.VeniceOfflinePushMonitorAccessor.OFFLINE_PUSH_SUB_PATH;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.ZkDataAccessException;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushmonitor.OfflinePushStatus;
import com.linkedin.venice.pushmonitor.PartitionStatus;
import com.linkedin.venice.pushmonitor.ReadOnlyPartitionStatus;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.Utils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class HelixOfflinePushMonitorAccessorTest {
  private String clusterName = Utils.getUniqueString("HelixOfflinePushMonitorAccessorTest");
  private ZkServerWrapper zk;
  private VeniceOfflinePushMonitorAccessor accessor;
  private String topic = "testTopic";
  private OfflinePushStatus offlinePushStatus =
      new OfflinePushStatus(topic, 3, 3, OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
  private ZkClient zkClient;

  @BeforeMethod
  public void setUp() {
    zk = ServiceFactory.getZkServer();
    String zkAddress = zk.getAddress();
    zkClient = ZkClientFactory.newZkClient(zkAddress);
    accessor = new VeniceOfflinePushMonitorAccessor(clusterName, zkClient, new HelixAdapterSerializer(), 1, 0);
  }

  @AfterMethod
  public void cleanUp() {
    zkClient.close();
    zk.close();
  }

  @Test
  public void testCreateOfflinePushStatus() {
    accessor.createOfflinePushStatusAndItsPartitionStatuses(offlinePushStatus);
    Assert.assertEquals(accessor.getOfflinePushStatusAndItsPartitionStatuses(topic), offlinePushStatus);
  }

  @Test
  public void testDeleteOfflinePushStatus() {
    accessor.createOfflinePushStatusAndItsPartitionStatuses(offlinePushStatus);
    accessor.deleteOfflinePushStatusAndItsPartitionStatuses(offlinePushStatus.getKafkaTopic());
    try {
      accessor.getOfflinePushStatusAndItsPartitionStatuses(topic);
      Assert.fail("Push status should be deleted.");
    } catch (VeniceException e) {

    }
  }

  @Test
  public void testUpdateOfflinePushStatus() {
    accessor.createOfflinePushStatusAndItsPartitionStatuses(offlinePushStatus);
    offlinePushStatus.updateStatus(ExecutionStatus.COMPLETED);
    accessor.updateOfflinePushStatus(offlinePushStatus);
    Assert.assertEquals(accessor.getOfflinePushStatusAndItsPartitionStatuses(topic), offlinePushStatus);
  }

  @Test
  public void testUpdateReplicaStatus() {
    accessor.createOfflinePushStatusAndItsPartitionStatuses(offlinePushStatus);
    int partitionId = 1;
    accessor.updateReplicaStatus(topic, partitionId, "i1", ExecutionStatus.COMPLETED, 0, "");
    accessor.updateReplicaStatus(topic, partitionId, "i2", ExecutionStatus.PROGRESS, 1000, "");
    offlinePushStatus.setPartitionStatus(
        ReadOnlyPartitionStatus.fromPartitionStatus(accessor.getPartitionStatus(topic, partitionId)));
    OfflinePushStatus remoteOfflinePushStatus = accessor.getOfflinePushStatusAndItsPartitionStatuses(topic);
    Assert.assertEquals(remoteOfflinePushStatus, offlinePushStatus);
  }

  @Test
  public void testUpdateReplicaStatusThatDoesNotExist() {
    int partitionId = 1;
    try {
      accessor.updateReplicaStatus(topic, partitionId, "i1", ExecutionStatus.COMPLETED, 0, "");
      Assert.assertNull(accessor.getPartitionStatus(topic, partitionId));
    } catch (ZkDataAccessException e) {
      Assert.fail("Should skip the update instead of throw a exception here.");
    }
  }

  @Test
  public void testLoadAllFromZk() {
    int offlinePushCount = 3;
    int partitionCount = 3;
    int replicationFactor = 3;
    Map<String, OfflinePushStatus> pushesMap = new HashMap<>();
    for (int i = 0; i < offlinePushCount; i++) {
      OfflinePushStatus push = new OfflinePushStatus(
          topic + i,
          partitionCount,
          replicationFactor,
          OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
      pushesMap.put(topic + i, push);
      accessor.createOfflinePushStatusAndItsPartitionStatuses(push);
      for (int j = 0; j < partitionCount; j++) {
        for (int k = 0; k < replicationFactor; k++) {
          accessor.updateReplicaStatus(
              topic + i,
              j,
              "i" + k,
              ExecutionStatus.COMPLETED,
              (long) (Math.random() * 10000),
              "");
        }
        push.setPartitionStatus(ReadOnlyPartitionStatus.fromPartitionStatus(accessor.getPartitionStatus(topic + i, j)));
      }
    }

    List<OfflinePushStatus> loadedPushes = accessor.loadOfflinePushStatusesAndPartitionStatuses();
    Assert.assertEquals(loadedPushes.size(), offlinePushCount);
    for (OfflinePushStatus loadedPush: loadedPushes) {
      Assert.assertEquals(loadedPush, pushesMap.get(loadedPush.getKafkaTopic()));
    }
  }

  @Test
  public void testGetPartitionStatusesWouldReturnCompletedSortedPartitionList() {
    final int partitionNum = 20;
    final int replicationFactor = 3;
    final String topicName = "test_store_v1";
    final String offlinePushStatusPath =
        HelixUtils.getHelixClusterZkPath(clusterName) + "/" + OFFLINE_PUSH_SUB_PATH + "/" + topicName;

    ZkBaseDataAccessor<OfflinePushStatus> offlinePushStatusAccessor = new ZkBaseDataAccessor<>(zkClient);
    ZkBaseDataAccessor<PartitionStatus> partitionStatusAccessor = new ZkBaseDataAccessor<>(zkClient);
    // build the offline push status ZK path for the test topic
    OfflinePushStatus completeOfflinePushStatus = new OfflinePushStatus(
        topicName,
        partitionNum,
        replicationFactor,
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
    HelixUtils.create(offlinePushStatusAccessor, offlinePushStatusPath, completeOfflinePushStatus);

    // build only part of the partitions: [0, 10, 2, 18]
    int[] partialPartitionIds = { 0, 10, 2, 18 };
    List<String> partitionPaths = new ArrayList<>(4);
    List<PartitionStatus> partitionStatuses = new ArrayList<>(4);

    for (int partitionId: partialPartitionIds) {
      partitionPaths.add(offlinePushStatusPath + "/" + partitionId);
      partitionStatuses.add(new PartitionStatus(partitionId));
    }
    HelixUtils.updateChildren(partitionStatusAccessor, partitionPaths, partitionStatuses);

    // Try to get the partition status with the complete partition number
    List<PartitionStatus> fullPartitionStatusList = accessor.getPartitionStatuses(topicName, partitionNum);
    // Verify the partition status list contains all partitions and is ordered by partition Id
    for (int i = 0; i < partitionNum; i++) {
      Assert.assertEquals(fullPartitionStatusList.get(i).getPartitionId(), i);
    }
  }

  @Test
  public void testGetOfflinePushStatusCreationTime() {
    accessor.createOfflinePushStatusAndItsPartitionStatuses(offlinePushStatus);
    Optional<Long> ctime = accessor.getOfflinePushStatusCreationTime(topic);
    Assert.assertTrue(ctime.isPresent());
    Assert.assertTrue(ctime.get() > 0);
  }
}
