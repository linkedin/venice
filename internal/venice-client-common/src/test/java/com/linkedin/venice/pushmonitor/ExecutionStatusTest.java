package com.linkedin.venice.pushmonitor;

import static com.linkedin.venice.pushmonitor.ExecutionStatus.DVC_INGESTION_ERROR_DISK_FULL;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.DVC_INGESTION_ERROR_MEMORY_LIMIT_REACHED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.DVC_INGESTION_ERROR_OTHER;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.DVC_INGESTION_ERROR_TOO_MANY_DEAD_INSTANCES;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.ERROR;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.STARTED;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

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
    assertEquals(ExecutionStatus.NOT_CREATED.getValue(), 0);
    assertEquals(ExecutionStatus.NEW.getValue(), 1);
    assertEquals(ExecutionStatus.STARTED.getValue(), 2);
    assertEquals(ExecutionStatus.PROGRESS.getValue(), 3);
    assertEquals(ExecutionStatus.END_OF_PUSH_RECEIVED.getValue(), 4);
    assertEquals(ExecutionStatus.START_OF_BUFFER_REPLAY_RECEIVED.getValue(), 5);
    assertEquals(ExecutionStatus.TOPIC_SWITCH_RECEIVED.getValue(), 6);
    assertEquals(ExecutionStatus.START_OF_INCREMENTAL_PUSH_RECEIVED.getValue(), 7);
    assertEquals(ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED.getValue(), 8);
    assertEquals(ExecutionStatus.DROPPED.getValue(), 9);
    assertEquals(ExecutionStatus.COMPLETED.getValue(), 10);
    assertEquals(ExecutionStatus.WARNING.getValue(), 11);
    assertEquals(ExecutionStatus.ERROR.getValue(), 12);
    assertEquals(ExecutionStatus.CATCH_UP_BASE_TOPIC_OFFSET_LAG.getValue(), 13);
    assertEquals(ExecutionStatus.ARCHIVED.getValue(), 14);
    assertEquals(ExecutionStatus.UNKNOWN.getValue(), 15);
    assertEquals(ExecutionStatus.NOT_STARTED.getValue(), 16);
    assertEquals(ExecutionStatus.DATA_RECOVERY_COMPLETED.getValue(), 17);
    assertEquals(ExecutionStatus.DVC_INGESTION_ERROR_DISK_FULL.getValue(), 18);
    assertEquals(ExecutionStatus.DVC_INGESTION_ERROR_MEMORY_LIMIT_REACHED.getValue(), 19);
    assertEquals(ExecutionStatus.DVC_INGESTION_ERROR_TOO_MANY_DEAD_INSTANCES.getValue(), 20);
    assertEquals(ExecutionStatus.DVC_INGESTION_ERROR_OTHER.getValue(), 21);
  }
}
