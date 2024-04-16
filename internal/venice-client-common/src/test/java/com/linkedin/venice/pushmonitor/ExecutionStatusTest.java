package com.linkedin.venice.pushmonitor;

import static com.linkedin.venice.pushmonitor.ExecutionStatus.ARCHIVED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.CATCH_UP_BASE_TOPIC_OFFSET_LAG;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.COMPLETED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.DATA_RECOVERY_COMPLETED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.DROPPED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.DVC_INGESTION_ERROR_DISK_FULL;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.DVC_INGESTION_ERROR_MEMORY_LIMIT_REACHED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.DVC_INGESTION_ERROR_OTHER;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.DVC_INGESTION_ERROR_TOO_MANY_DEAD_INSTANCES;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.END_OF_PUSH_RECEIVED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.ERROR;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.NEW;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.NOT_CREATED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.NOT_STARTED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.PROGRESS;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.STARTED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.START_OF_BUFFER_REPLAY_RECEIVED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.START_OF_INCREMENTAL_PUSH_RECEIVED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.TOPIC_SWITCH_RECEIVED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.UNKNOWN;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.WARNING;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.isDeterminedStatus;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.Test;


public class ExecutionStatusTest {
  @Test
  public void testisDVCIngestionError() {
    for (ExecutionStatus status: ExecutionStatus.values()) {
      if (status == DVC_INGESTION_ERROR_DISK_FULL || status == DVC_INGESTION_ERROR_MEMORY_LIMIT_REACHED
          || status == DVC_INGESTION_ERROR_TOO_MANY_DEAD_INSTANCES || status == DVC_INGESTION_ERROR_OTHER) {
        assertTrue(status.isDVCIngestionError(), status + " should not pass isDVCIngestionError()");
      } else {
        assertFalse(status.isDVCIngestionError(), status + " should pass isDVCIngestionError()");
      }
    }
  }

  @Test
  public void testisError() {
    for (ExecutionStatus status: ExecutionStatus.values()) {
      if (status == ERROR || status == DVC_INGESTION_ERROR_DISK_FULL
          || status == DVC_INGESTION_ERROR_MEMORY_LIMIT_REACHED || status == DVC_INGESTION_ERROR_TOO_MANY_DEAD_INSTANCES
          || status == DVC_INGESTION_ERROR_OTHER) {
        assertTrue(status.isError(), status + " should not pass isIngestionError()");
      } else {
        assertFalse(status.isError(), status + " should pass isIngestionError()");
      }
    }
  }

  @Test
  public void testIsErrorWithInputString() {
    assertTrue(ExecutionStatus.isError(ERROR.toString()));
    assertTrue(ExecutionStatus.isError(DVC_INGESTION_ERROR_DISK_FULL.toString()));
    assertTrue(ExecutionStatus.isError(DVC_INGESTION_ERROR_MEMORY_LIMIT_REACHED.toString()));
    assertTrue(ExecutionStatus.isError(DVC_INGESTION_ERROR_TOO_MANY_DEAD_INSTANCES.toString()));
    assertTrue(ExecutionStatus.isError(DVC_INGESTION_ERROR_OTHER.toString()));
    assertFalse(ExecutionStatus.isError(STARTED.toString()));
    assertTrue(ExecutionStatus.isError("error"));
    assertFalse(ExecutionStatus.isError("123"));
  }

  /**
   * Test to prevent unintentional changing of the values of ExecutionStatus
   * as values are persisted and used across services
   */
  @Test
  public void testExecutionStatusValue() {
    assertEquals(ExecutionStatus.values().length, 22);

    for (ExecutionStatus status: ExecutionStatus.values()) {
      assertEquals(status.getValue(), status.ordinal());
    }

    // check all the values in the enum one by one to make sure it's not changed
    assertEquals(NOT_CREATED.getValue(), 0);
    assertEquals(NEW.getValue(), 1);
    assertEquals(STARTED.getValue(), 2);
    assertEquals(PROGRESS.getValue(), 3);
    assertEquals(END_OF_PUSH_RECEIVED.getValue(), 4);
    assertEquals(START_OF_BUFFER_REPLAY_RECEIVED.getValue(), 5);
    assertEquals(TOPIC_SWITCH_RECEIVED.getValue(), 6);
    assertEquals(START_OF_INCREMENTAL_PUSH_RECEIVED.getValue(), 7);
    assertEquals(END_OF_INCREMENTAL_PUSH_RECEIVED.getValue(), 8);
    assertEquals(DROPPED.getValue(), 9);
    assertEquals(COMPLETED.getValue(), 10);
    assertEquals(WARNING.getValue(), 11);
    assertEquals(ERROR.getValue(), 12);
    assertEquals(CATCH_UP_BASE_TOPIC_OFFSET_LAG.getValue(), 13);
    assertEquals(ARCHIVED.getValue(), 14);
    assertEquals(UNKNOWN.getValue(), 15);
    assertEquals(NOT_STARTED.getValue(), 16);
    assertEquals(DATA_RECOVERY_COMPLETED.getValue(), 17);
    assertEquals(DVC_INGESTION_ERROR_DISK_FULL.getValue(), 18);
    assertEquals(DVC_INGESTION_ERROR_MEMORY_LIMIT_REACHED.getValue(), 19);
    assertEquals(DVC_INGESTION_ERROR_TOO_MANY_DEAD_INSTANCES.getValue(), 20);
    assertEquals(DVC_INGESTION_ERROR_OTHER.getValue(), 21);
  }

  @Test
  public void testIsUsedByDaVinciClientOnly() {
    Set<ExecutionStatus> dvcOnly = new HashSet<>();
    dvcOnly.add(DVC_INGESTION_ERROR_DISK_FULL);
    dvcOnly.add(DVC_INGESTION_ERROR_MEMORY_LIMIT_REACHED);
    dvcOnly.add(DVC_INGESTION_ERROR_TOO_MANY_DEAD_INSTANCES);
    dvcOnly.add(DVC_INGESTION_ERROR_OTHER);

    for (ExecutionStatus status: ExecutionStatus.values()) {
      if (dvcOnly.contains(status)) {
        assertTrue(status.isUsedByDaVinciClientOnly(), status + " should not pass isUsedByDaVinciClientOnly()");
      } else {
        assertFalse(status.isUsedByDaVinciClientOnly(), status + " should pass isUsedByDaVinciClientOnly()");
      }
    }
  }

  @Test
  public void testIsTerminal() {
    Set<ExecutionStatus> terminalStatuses = new HashSet<>();
    terminalStatuses.add(END_OF_INCREMENTAL_PUSH_RECEIVED);
    terminalStatuses.add(COMPLETED);
    terminalStatuses.add(WARNING);
    terminalStatuses.add(ERROR);
    terminalStatuses.add(ARCHIVED);
    terminalStatuses.add(DVC_INGESTION_ERROR_DISK_FULL);
    terminalStatuses.add(DVC_INGESTION_ERROR_MEMORY_LIMIT_REACHED);
    terminalStatuses.add(DVC_INGESTION_ERROR_TOO_MANY_DEAD_INSTANCES);
    terminalStatuses.add(DVC_INGESTION_ERROR_OTHER);

    for (ExecutionStatus status: ExecutionStatus.values()) {
      if (terminalStatuses.contains(status)) {
        assertTrue(status.isTerminal(), status + " should not pass isTerminal()");
      } else {
        assertFalse(status.isTerminal(), status + " should pass isTerminal()");
      }
    }
  }

  @Test
  public void testIsDeterminedStatus() {
    Set<ExecutionStatus> determinedStatuses = new HashSet<>();
    determinedStatuses.add(STARTED);
    determinedStatuses.add(COMPLETED);
    determinedStatuses.add(ERROR);
    determinedStatuses.add(DROPPED);
    determinedStatuses.add(END_OF_PUSH_RECEIVED);
    determinedStatuses.add(DVC_INGESTION_ERROR_DISK_FULL);
    determinedStatuses.add(DVC_INGESTION_ERROR_MEMORY_LIMIT_REACHED);
    determinedStatuses.add(DVC_INGESTION_ERROR_TOO_MANY_DEAD_INSTANCES);
    determinedStatuses.add(DVC_INGESTION_ERROR_OTHER);

    for (ExecutionStatus status: ExecutionStatus.values()) {
      if (determinedStatuses.contains(status)) {
        assertTrue(isDeterminedStatus(status), status + " should not pass isDeterminedStatus()");
      } else {
        assertFalse(isDeterminedStatus(status), status + " should pass isDeterminedStatus()");
      }
    }
  }

  @Test
  public void testRootStatus() {
    Map<ExecutionStatus, ExecutionStatus> rootStatusMap = new HashMap<>();
    rootStatusMap.put(DVC_INGESTION_ERROR_DISK_FULL, ERROR);
    rootStatusMap.put(DVC_INGESTION_ERROR_MEMORY_LIMIT_REACHED, ERROR);
    rootStatusMap.put(DVC_INGESTION_ERROR_TOO_MANY_DEAD_INSTANCES, ERROR);
    rootStatusMap.put(DVC_INGESTION_ERROR_OTHER, ERROR);

    // root status of all other statuses should be the status itself by default
    for (ExecutionStatus status: ExecutionStatus.values()) {
      assertEquals(status.getRootStatus(), rootStatusMap.getOrDefault(status, status));
    }
  }
}
