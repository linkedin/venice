package com.linkedin.venice.pushmonitor;

import static com.linkedin.venice.pushmonitor.ExecutionStatus.DVC_INGESTION_ERROR_DISK_FULL;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.DVC_INGESTION_ERROR_MEMORY_LIMIT_REACHED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.DVC_INGESTION_ERROR_OTHER;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.DVC_INGESTION_ERROR_TOO_MANY_DEAD_INSTANCES;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.ERROR;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.STARTED;
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
}
