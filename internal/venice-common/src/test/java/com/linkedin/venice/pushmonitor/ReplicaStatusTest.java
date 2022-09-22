package com.linkedin.venice.pushmonitor;

import static com.linkedin.venice.pushmonitor.ExecutionStatus.COMPLETED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.END_OF_PUSH_RECEIVED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.ERROR;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.PROGRESS;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.STARTED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.START_OF_INCREMENTAL_PUSH_RECEIVED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.TOPIC_SWITCH_RECEIVED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.isIncrementalPushStatus;

import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ReplicaStatusTest {
  private String instanceId = "testInstance";

  @Test
  public void testCreateReplicaStatus() {
    ReplicaStatus replicaStatus = new ReplicaStatus(instanceId);
    Assert.assertEquals(replicaStatus.getInstanceId(), instanceId);
    Assert.assertEquals(replicaStatus.getCurrentStatus(), STARTED);
    Assert.assertEquals(replicaStatus.getCurrentProgress(), 0);
  }

  private void testStatusesUpdate(ExecutionStatus from, ExecutionStatus... statuses) {
    for (ExecutionStatus status: statuses) {
      ReplicaStatus replicaStatus = new ReplicaStatus(instanceId);
      replicaStatus.setCurrentStatus(from);
      replicaStatus.updateStatus(status);
      Assert.assertEquals(replicaStatus.getCurrentStatus(), status);
    }
  }

  @Test
  public void testUpdateStatusFromSTARTED() {
    testStatusesUpdate(STARTED, PROGRESS, ERROR, COMPLETED);
  }

  @Test
  public void testUpdateStatusFromPROGRESS() {
    testStatusesUpdate(PROGRESS, STARTED, PROGRESS, ERROR, COMPLETED);
  }

  @Test
  public void testUpdateStatusFromERROR() {
    testStatusesUpdate(ERROR, STARTED);
  }

  @Test
  public void testUpdateStatusFromEndOfPushReceived() {
    testStatusesUpdate(END_OF_PUSH_RECEIVED, STARTED, ERROR, COMPLETED, TOPIC_SWITCH_RECEIVED);
  }

  @Test
  public void testUpdateStatusFromTopicSwitchReceived() {
    /**
     * For reprocessing, it's possible that END_OF_PUSH_RECEIVED status will come after a TOPIC_SWITCH status
     */
    testStatusesUpdate(TOPIC_SWITCH_RECEIVED, END_OF_PUSH_RECEIVED, STARTED, ERROR, PROGRESS, COMPLETED);
  }

  @Test
  public void testUpdateStatusFromCOMPLETED() {
    testStatusesUpdate(
        COMPLETED,
        STARTED,
        ERROR,
        START_OF_INCREMENTAL_PUSH_RECEIVED,
        END_OF_INCREMENTAL_PUSH_RECEIVED,
        TOPIC_SWITCH_RECEIVED);
  }

  @Test
  public void testStatusHistory() {
    ReplicaStatus replicaStatus = new ReplicaStatus(instanceId);
    replicaStatus.updateStatus(STARTED);
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
  public void testStatusHistorySaveLastValidStatus() {
    ReplicaStatus replicaStatus = new ReplicaStatus(instanceId);
    replicaStatus.updateStatus(COMPLETED);
    replicaStatus.updateStatus(START_OF_INCREMENTAL_PUSH_RECEIVED);
    for (int i = 0; i <= ReplicaStatus.MAX_HISTORY_LENGTH + 1; i++) {
      replicaStatus.updateStatus(STARTED);
    }
    replicaStatus.updateStatus(COMPLETED);
    replicaStatus.updateStatus(END_OF_INCREMENTAL_PUSH_RECEIVED);

    Assert.assertEquals(replicaStatus.getStatusHistory().size(), replicaStatus.MAX_HISTORY_LENGTH);
    Assert.assertEquals(replicaStatus.getStatusHistory().get(0).getStatus(), START_OF_INCREMENTAL_PUSH_RECEIVED);
    Assert.assertEquals(
        replicaStatus.getStatusHistory().get(replicaStatus.MAX_HISTORY_LENGTH - 1).getStatus(),
        END_OF_INCREMENTAL_PUSH_RECEIVED);
    Assert.assertEquals(
        replicaStatus.getStatusHistory().get(replicaStatus.MAX_HISTORY_LENGTH - 2).getStatus(),
        COMPLETED);
  }

  @Test
  public void testIncrementalPushStatesGotRemovedFirst() {
    ReplicaStatus replicaStatus = new ReplicaStatus(instanceId);
    replicaStatus.updateStatus(STARTED);
    replicaStatus.updateStatus(END_OF_PUSH_RECEIVED);
    replicaStatus.updateStatus(COMPLETED);

    for (int i = 0; i < ReplicaStatus.MAX_HISTORY_LENGTH; i++) {
      replicaStatus.updateStatus(START_OF_INCREMENTAL_PUSH_RECEIVED, "testInc1");
    }
    replicaStatus.updateStatus(END_OF_INCREMENTAL_PUSH_RECEIVED, "testInc1");
    replicaStatus.updateStatus(TOPIC_SWITCH_RECEIVED);

    // since we are adding another inc push and the max length is reached, the previous inc push status should be
    // removed.
    replicaStatus.updateStatus(START_OF_INCREMENTAL_PUSH_RECEIVED, "testInc2");
    replicaStatus.updateStatus(END_OF_INCREMENTAL_PUSH_RECEIVED, "testInc2");

    List<StatusSnapshot> statusHistory = replicaStatus.getStatusHistory();
    Assert.assertEquals(statusHistory.size(), ReplicaStatus.MAX_HISTORY_LENGTH);
    boolean containsCompleted = false, containsEOP = false, containsTS = false;
    for (StatusSnapshot statusSnapshot: statusHistory) {
      if (statusSnapshot.getStatus() == COMPLETED) {
        containsCompleted = true;
      } else if (statusSnapshot.getStatus() == END_OF_PUSH_RECEIVED) {
        containsEOP = true;
      } else if (statusSnapshot.getStatus() == TOPIC_SWITCH_RECEIVED) {
        containsTS = true;
      }
    }
    Assert.assertTrue(containsCompleted && containsEOP && containsTS);
  }

  @Test
  public void testCurrentIncPushVersionStatusGotSaved() {
    ReplicaStatus replicaStatus = new ReplicaStatus(instanceId);
    // update (max length + 1) statuses to the replica status history
    replicaStatus.updateStatus(STARTED);
    for (int i = 0; i < ReplicaStatus.MAX_HISTORY_LENGTH; i++) {
      replicaStatus.updateStatus(START_OF_INCREMENTAL_PUSH_RECEIVED, "testInc1");
    }
    // Inc push statuses which share the current inc push version would be saved.
    List<StatusSnapshot> statusHistory = replicaStatus.getStatusHistory();
    Assert.assertEquals(statusHistory.size(), replicaStatus.MAX_HISTORY_LENGTH);
    statusHistory.forEach((i) -> Assert.assertTrue(isIncrementalPushStatus(i.getStatus())));
  }

  @Test
  public void testStatusHistoryWithLotsOfProgressStatus() {
    ReplicaStatus replicaStatus = new ReplicaStatus(instanceId);
    replicaStatus.updateStatus(STARTED);
    for (int i = 0; i < ReplicaStatus.MAX_HISTORY_LENGTH * 2; i++) {
      replicaStatus.updateStatus(PROGRESS);
    }
    Assert.assertEquals(replicaStatus.getCurrentStatus(), PROGRESS);
    Assert.assertEquals(
        replicaStatus.getStatusHistory().size(),
        2,
        "PROGRESS should be added into history if the previous status is also PROGRESS.");
  }
}
