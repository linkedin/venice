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
  static final int NUM_OF_PARTITION = 5;
  static final int REPLICATION_FACTOR = 3;
  static final String KAFKA_TOPIC = "test_v1";

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
        KAFKA_TOPIC,
        NUM_OF_PARTITION,
        REPLICATION_FACTOR,
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);

    for (int i = 0; i < NUM_OF_PARTITION; i++) {
      PartitionStatus partition = new PartitionStatus(i);
      for (int j = 0; j < REPLICATION_FACTOR; j++) {
        if (isNormal) {
          partition.updateReplicaStatus("instanceId-" + j, ExecutionStatus.STARTED);
          partition.updateReplicaStatus("instanceId-" + j, ExecutionStatus.END_OF_PUSH_RECEIVED);
          partition.updateReplicaStatus("instanceId-" + j, ExecutionStatus.TOPIC_SWITCH_RECEIVED);
        }
        partition.updateReplicaStatus("instanceId-" + j, ExecutionStatus.COMPLETED);
        partition.updateReplicaStatus("instanceId-" + j, ExecutionStatus.STARTED);
        partition.updateReplicaStatus("instanceId-" + j, ExecutionStatus.COMPLETED);
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
    Assert.assertEquals(result.getPartitionDetails().size(), NUM_OF_PARTITION);
    for (int i = 0; i < NUM_OF_PARTITION; i++) {
      Assert.assertEquals(result.getPartitionDetails().get(i).getReplicaDetails().size(), REPLICATION_FACTOR);
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
