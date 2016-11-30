package com.linkedin.venice.pushmonitor;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.job.ExecutionStatus;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.linkedin.venice.job.ExecutionStatus.*;


public class ReplicaStatusTest {
  private String instanceId = "testInstance";

  @Test
  public void testCreateReplicaStatus() {
    ReplicaStatus replicaStatus = new ReplicaStatus(instanceId);
    Assert.assertEquals(replicaStatus.getInstanceId(), instanceId);
    Assert.assertEquals(replicaStatus.getCurrentStatus(), STARTED);
    Assert.assertEquals(replicaStatus.getCurrentProgress(), 0);
  }

  private void testValidTargetStatuses(ExecutionStatus from, ExecutionStatus... statuses) {
    for (ExecutionStatus status : statuses) {
      ReplicaStatus replicaStatus = new ReplicaStatus(instanceId);
      replicaStatus.setCurrentStatus(from);
      replicaStatus.updateStatus(status);
      Assert.assertEquals(replicaStatus.getCurrentStatus(), status, status + " should be valid from:" + from);
    }
  }

  public void testInvalidTargetStatuses(ExecutionStatus from, ExecutionStatus... statuses) {
    for (ExecutionStatus status : statuses) {
      ReplicaStatus replicaStatus = new ReplicaStatus(instanceId);
      replicaStatus.setCurrentStatus(from);
      try {
        replicaStatus.updateStatus(status);
        Assert.fail(status + " is invalid from:" + from);
      } catch (VeniceException e) {
        //expected.
      }
    }
  }

  @Test
  public void testUpdateStatusFromSTARTED() {
    testValidTargetStatuses(STARTED, PROGRESS, ERROR, COMPLETED);
  }

  @Test
  public void testUpdateStatusFromPROGRESS() {
    testValidTargetStatuses(PROGRESS, STARTED, PROGRESS, ERROR, COMPLETED);
  }

  @Test
  public void testUpdateStatusFromERROR() {
    testValidTargetStatuses(ERROR, STARTED);
    testInvalidTargetStatuses(ERROR, PROGRESS, ERROR, COMPLETED);
  }

  @Test
  public void testUpdateStatusFromCOMPLETED() {
    testValidTargetStatuses(COMPLETED, STARTED);
    testInvalidTargetStatuses(COMPLETED, PROGRESS, ERROR, COMPLETED);
  }

  @Test
  public void testStatusHistory() {
    ReplicaStatus replicaStatus = new ReplicaStatus(instanceId);
    replicaStatus.updateStatus(PROGRESS);
    replicaStatus.updateStatus(COMPLETED);

    List<StatusSnapshot> history = replicaStatus.getStatusHistory();
    Assert.assertEquals(history.size(), 3);
    Assert.assertEquals(history.get(0).getStatus(), STARTED);
    Assert.assertEquals(history.get(1).getStatus(), PROGRESS);
    Assert.assertEquals(history.get(2).getStatus(), COMPLETED);
  }

  @Test
  public void testStatusHistoryTooLong() {
    ReplicaStatus replicaStatus = new ReplicaStatus(instanceId);
    for (int i = 0; i < ReplicaStatus.MAX_HISTORY_LENGTH * 2; i++) {
      replicaStatus.updateStatus(STARTED);
    }
    Assert.assertEquals(replicaStatus.getStatusHistory().size(), replicaStatus.MAX_HISTORY_LENGTH);
  }

  @Test
  public void testStatusHistoryWithLotsOfProgressStatus() {
    ReplicaStatus replicaStatus = new ReplicaStatus(instanceId);
    for (int i = 0; i < ReplicaStatus.MAX_HISTORY_LENGTH * 2; i++) {
      replicaStatus.updateStatus(PROGRESS);
    }
    Assert.assertEquals(replicaStatus.getCurrentStatus(), PROGRESS);
    Assert.assertEquals(replicaStatus.getStatusHistory().size(), 2,
        "PROGRESS should be added into history if the previous status is also PROGRESS.");
  }
}