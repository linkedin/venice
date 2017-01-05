package com.linkedin.venice.helix;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.job.ExecutionStatus;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.pushmonitor.OfflinePushStatus;
import com.linkedin.venice.pushmonitor.ReadonlyPartitionStatus;
import com.linkedin.venice.utils.TestUtils;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.manager.zk.ZkClient;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class HelixOfflinePushMonitorAccessorTest {
  private String clusterName = TestUtils.getUniqueString("HelixOfflinePushMonitorAccessorTest");
  private ZkServerWrapper zk;
  private HelixOfflinePushMonitorAccessor accessor;
  private String topic = "testTopic";
  private OfflinePushStatus offlinePushStatus =
      new OfflinePushStatus(topic, 3, 3, OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);

  @BeforeMethod
  public void setup() {
    zk = ServiceFactory.getZkServer();
    String zkAddress = zk.getAddress();
    ZkClient zkClient = new ZkClient(zkAddress);
    accessor = new HelixOfflinePushMonitorAccessor(clusterName, zkClient, new HelixAdapterSerializer());
  }

  @AfterMethod
  public void cleanup() {
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
    accessor.deleteOfflinePushStatusAndItsPartitionStatuses(offlinePushStatus);
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
    accessor.updateReplicaStatus(topic, partitionId, "i1", ExecutionStatus.COMPLETED, 0);
    accessor.updateReplicaStatus(topic, partitionId, "i2", ExecutionStatus.PROGRESS, 1000);
    offlinePushStatus.setPartitionStatus(
        ReadonlyPartitionStatus.fromPartitionStatus(accessor.getPartitionStatus(topic, partitionId)));
    OfflinePushStatus remoteOfflinePushStatus = accessor.getOfflinePushStatusAndItsPartitionStatuses(topic);
    Assert.assertEquals(remoteOfflinePushStatus, offlinePushStatus);
  }

  @Test
  public void testLoadAllFromZk() {
    int offlinePushCount = 3;
    int partitionCount = 3;
    int replicationFactor = 3;
    Map<String, OfflinePushStatus> pushesMap = new HashMap<>();
    for (int i = 0; i < offlinePushCount; i++) {
      OfflinePushStatus push = new OfflinePushStatus(topic + i, partitionCount, replicationFactor,
          OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
      pushesMap.put(topic + i, push);
      accessor.createOfflinePushStatusAndItsPartitionStatuses(push);
      for (int j = 0; j < partitionCount; j++) {
        for (int k = 0; k < replicationFactor; k++) {
          accessor
              .updateReplicaStatus(topic + i, j, "i" + k, ExecutionStatus.COMPLETED, (long) (Math.random() * 10000));
        }
        push.setPartitionStatus(ReadonlyPartitionStatus.fromPartitionStatus(accessor.getPartitionStatus(topic + i, j)));
      }
    }

    List<OfflinePushStatus> loadedPushes = accessor.loadOfflinePushStatusesAndPartitionStatuses();
    for (OfflinePushStatus loadedPush : loadedPushes) {
      Assert.assertEquals(loadedPush, pushesMap.get(loadedPush.getKafkaTopic()));
    }
  }
}
