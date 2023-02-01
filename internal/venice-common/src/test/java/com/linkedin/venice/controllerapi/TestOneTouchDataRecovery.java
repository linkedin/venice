package com.linkedin.venice.controllerapi;

import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.RegionPushDetails;
import com.linkedin.venice.meta.ReplicaDetail;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushmonitor.OfflinePushStatus;
import com.linkedin.venice.pushmonitor.PartitionStatus;
import java.time.LocalDateTime;
import org.apache.commons.lang.StringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestOneTouchDataRecovery {
  final int numOfPartition = 5;
  final int replicationFactor = 3;
  final String kafkaTopic = "test_v1";

  @Test
  public void testDataRecoveryDataStructure() {
    LocalDateTime now = LocalDateTime.now();
    RegionPushDetails result = buildPushStatus(now, true);
    verifyPushStatus(result, now, true);

    now = LocalDateTime.now();
    result = buildPushStatus(now, false);
    verifyPushStatus(result, now, false);
  }

  private RegionPushDetails buildPushStatus(LocalDateTime timestamp, boolean isNormal) {
    OfflinePushStatus status = new OfflinePushStatus(
        kafkaTopic,
        numOfPartition,
        replicationFactor,
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);

    for (int i = 0; i < numOfPartition; i++) {
      PartitionStatus partition = new PartitionStatus(i);
      for (int j = 0; j < replicationFactor; j++) {
        if (isNormal) {
          partition.updateReplicaStatus("instanceId-" + j, ExecutionStatus.STARTED, StringUtils.EMPTY);
          partition.updateReplicaStatus("instanceId-" + j, ExecutionStatus.END_OF_PUSH_RECEIVED, StringUtils.EMPTY);
          partition.updateReplicaStatus("instanceId-" + j, ExecutionStatus.TOPIC_SWITCH_RECEIVED, StringUtils.EMPTY);
        }
        partition.updateReplicaStatus("instanceId-" + j, ExecutionStatus.COMPLETED, StringUtils.EMPTY);
        partition.updateReplicaStatus("instanceId-" + j, ExecutionStatus.STARTED, StringUtils.EMPTY);
        partition.updateReplicaStatus("instanceId-" + j, ExecutionStatus.COMPLETED, StringUtils.EMPTY);
      }
      status.setPartitionStatus(partition);
    }

    RegionPushDetails result = new RegionPushDetails();
    result.addPartitionDetails(status);
    result.setPushStartTimestamp(timestamp.toString());
    result.setPushEndTimestamp(timestamp.plusHours(1).toString());
    return result;
  }

  private void verifyPushStatus(final RegionPushDetails result, LocalDateTime timestamp, boolean isNormal) {
    Assert.assertEquals(result.getPartitionDetails().size(), numOfPartition);
    for (int i = 0; i < numOfPartition; i++) {
      Assert.assertEquals(result.getPartitionDetails().get(i).getReplicaDetails().size(), replicationFactor);
      for (ReplicaDetail replica: result.getPartitionDetails().get(i).getReplicaDetails()) {
        Assert.assertNotEquals(replica.getInstanceId(), StringUtils.EMPTY);
        if (isNormal) {
          Assert.assertNotEquals(replica.getPushStartDateTime(), StringUtils.EMPTY);
          Assert.assertNotEquals(replica.getPushEndDateTime(), StringUtils.EMPTY);
        } else {
          Assert.assertEquals(replica.getPushStartDateTime(), StringUtils.EMPTY);
          Assert.assertEquals(replica.getPushEndDateTime(), StringUtils.EMPTY);
        }
      }
    }
    Assert.assertEquals(result.getPushStartTimestamp(), timestamp.toString());
    Assert.assertEquals(result.getPushEndTimestamp(), timestamp.plusHours(1).toString());
  }
}
