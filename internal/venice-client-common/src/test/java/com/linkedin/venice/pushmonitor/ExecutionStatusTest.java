package com.linkedin.venice.pushmonitor;

import static com.linkedin.venice.pushmonitor.ExecutionStatus.DVC_INGESTION_ERROR_DISK_FULL;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.DVC_INGESTION_ERROR_MEMORY_LIMIT_REACHED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.DVC_INGESTION_ERROR_OTHER;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.ERROR;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;


public class ExecutionStatusTest {
  @Test
  public void testisDVCIngestionError() {
    for (ExecutionStatus status: ExecutionStatus.values()) {
      if (status == DVC_INGESTION_ERROR_DISK_FULL || status == DVC_INGESTION_ERROR_MEMORY_LIMIT_REACHED
          || status == DVC_INGESTION_ERROR_OTHER) {
        assertTrue(status.isDVCIngestionError(), status + " should not pass isDVCIngestionError()");
      } else {
        assertFalse(status.isDVCIngestionError(), status + " should pass isDVCIngestionError()");
      }
    }
  }

  @Test
  public void testisIngestionError() {
    for (ExecutionStatus status: ExecutionStatus.values()) {
      if (status == ERROR || status == DVC_INGESTION_ERROR_DISK_FULL
          || status == DVC_INGESTION_ERROR_MEMORY_LIMIT_REACHED || status == DVC_INGESTION_ERROR_OTHER) {
        assertTrue(status.isIngestionError(), status + " should not pass isIngestionError()");
      } else {
        assertFalse(status.isIngestionError(), status + " should pass isIngestionError()");
      }
    }
  }
}
