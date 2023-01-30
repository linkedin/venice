package com.linkedin.venice.controllerapi;

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.RegionPushDetails;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushmonitor.OfflinePushStatus;
import com.linkedin.venice.pushmonitor.PartitionStatus;
import com.linkedin.venice.pushmonitor.StatusSnapshot;
import com.linkedin.venice.utils.TestUtils;
import java.time.LocalDateTime;
import org.apache.commons.lang.StringUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestOneTouchDataRecovery {
  private VeniceHelixAdmin internalAdmin;
  static final String clusterName = "test-cluster";

  @BeforeMethod
  public void setupTestCase() {
    internalAdmin = mock(VeniceHelixAdmin.class);
  }

  @Test
  public void testDataRecoveryAPIs() {
    final String storeName = "test";
    final String owner = "test";
    final int numOfPartition = 5;
    final int replicationFactor = 3;
    final String kafkaTopic = "test_v1";

    OfflinePushStatus status = new OfflinePushStatus(
        kafkaTopic,
        numOfPartition,
        replicationFactor,
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
    LocalDateTime now = LocalDateTime.now();

    for (int i = 0; i < numOfPartition; i++) {
      PartitionStatus partition = new PartitionStatus(i);
      for (int j = 0; j < replicationFactor; j++) {
        partition.updateReplicaStatus("instanceId-" + j, ExecutionStatus.STARTED, StringUtils.EMPTY);
        partition.updateReplicaStatus("instanceId-" + j, ExecutionStatus.COMPLETED, StringUtils.EMPTY);
      }
      status.setPartitionStatus(partition);
    }

    status.getStatusHistory().add(new StatusSnapshot(ExecutionStatus.STARTED, now.toString()));
    status.getStatusHistory().add(new StatusSnapshot(ExecutionStatus.COMPLETED, now.plusHours(1).toString()));
    doReturn(status).when(internalAdmin).retrievePushStatus(anyString(), anyString());

    Store s = TestUtils.createTestStore(storeName, owner, System.currentTimeMillis());
    when(internalAdmin.getRegionPushDetails(anyString(), anyString(), anyBoolean())).thenCallRealMethod();
    doReturn(s).when(internalAdmin).getStore(anyString(), anyString());

    RegionPushDetails details = internalAdmin.getRegionPushDetails(clusterName, storeName, true);
    Assert.assertEquals(details.getPushEndTimestamp(), now.plusHours(1).toString());
    Assert.assertEquals(details.getCurrentVersion().intValue(), Store.NON_EXISTING_VERSION);
    Assert.assertEquals(details.getPartitionDetails().size(), numOfPartition);
    for (int i = 0; i < numOfPartition; i++) {
      Assert.assertEquals(details.getPartitionDetails().get(i).getReplicaDetails().size(), replicationFactor);
    }
  }
}
