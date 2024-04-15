package com.linkedin.davinci;

import static com.linkedin.venice.pushmonitor.ExecutionStatus.DVC_INGESTION_ERROR_OTHER;
import static com.linkedin.venice.utils.DataProviderUtils.allPermutationGenerator;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import com.linkedin.venice.exceptions.MemoryLimitExhaustedException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class DaVinciBackendTest {
  @DataProvider(name = "DvcErrorExecutionStatus")
  public static Object[][] dvcErrorExecutionStatus() {
    return allPermutationGenerator((permutation) -> {
      ExecutionStatus status = (ExecutionStatus) permutation[0];
      return status.isDVCIngestionError();
    }, ExecutionStatus.values());
  }

  @Test(dataProvider = "DvcErrorExecutionStatus")
  public void testGetDaVinciErrorStatus(ExecutionStatus executionStatus) {
    String message;
    VeniceException veniceException;
    switch (executionStatus) {
      case DVC_INGESTION_ERROR_DISK_FULL:
        message = "Disk is full";
        veniceException = new VeniceException("Disk is full");
        break;
      case DVC_INGESTION_ERROR_MEMORY_LIMIT_REACHED:
        message = "Memory limit reached";
        veniceException = new MemoryLimitExhaustedException("Memory limit reached");
        break;
      case DVC_INGESTION_ERROR_TOO_MANY_DEAD_INSTANCES:
        message = "Too many dead instances";
        veniceException = new VeniceException("Too many dead instances");
        break;
      case DVC_INGESTION_ERROR_OTHER:
        message = "Some other error";
        veniceException = new VeniceException("Some other error");
        break;
      default:
        fail("Unexpected execution status: " + executionStatus);
        return;
    }
    assertEquals(
        DaVinciBackend.getDaVinciErrorStatus(message, veniceException),
        executionStatus.equals(ExecutionStatus.DVC_INGESTION_ERROR_TOO_MANY_DEAD_INSTANCES)
            ? DVC_INGESTION_ERROR_OTHER
            : executionStatus);
  }

  @Test(dataProvider = "DvcErrorExecutionStatus")
  public void testGetDaVinciErrorStatusWithInvalidCases(ExecutionStatus executionStatus) {
    String message;
    VeniceException veniceException;
    switch (executionStatus) {
      case DVC_INGESTION_ERROR_DISK_FULL:
        message = "Disk is not full";
        veniceException = new VeniceException("Disk is not full");
        break;
      case DVC_INGESTION_ERROR_MEMORY_LIMIT_REACHED:
        message = "Memory limit reached";
        veniceException = new VeniceException("Memory limit reached");
        break;
      case DVC_INGESTION_ERROR_TOO_MANY_DEAD_INSTANCES:
        message = "Too many dead instances";
        veniceException = new VeniceException("Too many dead instances");
        break;
      case DVC_INGESTION_ERROR_OTHER:
        message = "Some other error";
        veniceException = new VeniceException("Some other error");
        break;
      default:
        fail("Unexpected execution status: " + executionStatus);
        return;
    }

    assertEquals(DaVinciBackend.getDaVinciErrorStatus(message, veniceException), DVC_INGESTION_ERROR_OTHER);
  }

}
