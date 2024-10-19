package com.linkedin.venice.pushmonitor;

import static com.linkedin.venice.pushmonitor.ExecutionStatus.ARCHIVED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.COMPLETED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.DROPPED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.DVC_INGESTION_ERROR_DISK_FULL;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.DVC_INGESTION_ERROR_MEMORY_LIMIT_REACHED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.DVC_INGESTION_ERROR_OTHER;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.DVC_INGESTION_ERROR_TOO_MANY_DEAD_INSTANCES;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.END_OF_PUSH_RECEIVED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.ERROR;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.STARTED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.WARNING;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.isDeterminedStatus;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.alpini.base.misc.CollectionUtil;
import com.linkedin.venice.utils.VeniceEnumValueTest;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ExecutionStatusTest extends VeniceEnumValueTest<ExecutionStatus> {
  public ExecutionStatusTest() {
    super(ExecutionStatus.class);
  }

  @Override
  protected Map<Integer, ExecutionStatus> expectedMapping() {
    return CollectionUtil.<Integer, ExecutionStatus>mapBuilder()
        .put(0, ExecutionStatus.NOT_CREATED)
        .put(1, ExecutionStatus.NEW)
        .put(2, ExecutionStatus.STARTED)
        .put(3, ExecutionStatus.PROGRESS)
        .put(4, ExecutionStatus.END_OF_PUSH_RECEIVED)
        .put(5, ExecutionStatus.START_OF_BUFFER_REPLAY_RECEIVED)
        .put(6, ExecutionStatus.TOPIC_SWITCH_RECEIVED)
        .put(7, ExecutionStatus.START_OF_INCREMENTAL_PUSH_RECEIVED)
        .put(8, ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED)
        .put(9, ExecutionStatus.DROPPED)
        .put(10, ExecutionStatus.COMPLETED)
        .put(11, ExecutionStatus.WARNING)
        .put(12, ExecutionStatus.ERROR)
        .put(13, ExecutionStatus.CATCH_UP_BASE_TOPIC_OFFSET_LAG)
        .put(14, ExecutionStatus.ARCHIVED)
        .put(15, ExecutionStatus.UNKNOWN)
        .put(16, ExecutionStatus.NOT_STARTED)
        .put(17, ExecutionStatus.DATA_RECOVERY_COMPLETED)
        .put(18, ExecutionStatus.DVC_INGESTION_ERROR_DISK_FULL)
        .put(19, ExecutionStatus.DVC_INGESTION_ERROR_MEMORY_LIMIT_REACHED)
        .put(20, ExecutionStatus.DVC_INGESTION_ERROR_TOO_MANY_DEAD_INSTANCES)
        .put(21, ExecutionStatus.DVC_INGESTION_ERROR_OTHER)
        .build();
  }

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

  @Test
  public void testErrorExecutionStatus() {
    for (ExecutionStatus status: ExecutionStatus.values()) {
      if (status == ERROR) {
        Assert.assertTrue(ExecutionStatus.isError(status));
      } else {
        Assert.assertFalse(ExecutionStatus.isError(status));
      }
    }
  }
}
