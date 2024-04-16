package com.linkedin.davinci;

import static com.linkedin.venice.pushmonitor.ExecutionStatus.DVC_INGESTION_ERROR_OTHER;
import static com.linkedin.venice.utils.DataProviderUtils.allPermutationGenerator;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import com.linkedin.venice.exceptions.DiskLimitExhaustedException;
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
    VeniceException veniceException;
    switch (executionStatus) {
      case DVC_INGESTION_ERROR_DISK_FULL:
        veniceException = new DiskLimitExhaustedException("test");
        break;
      case DVC_INGESTION_ERROR_MEMORY_LIMIT_REACHED:
        veniceException = new MemoryLimitExhaustedException("test");
        break;
      case DVC_INGESTION_ERROR_TOO_MANY_DEAD_INSTANCES:
      case DVC_INGESTION_ERROR_OTHER:
        veniceException = new VeniceException("test");
        break;
      default:
        fail("Unexpected execution status: " + executionStatus);
        return;
    }
    assertEquals(
        DaVinciBackend.getDaVinciErrorStatus(veniceException),
        executionStatus.equals(ExecutionStatus.DVC_INGESTION_ERROR_TOO_MANY_DEAD_INSTANCES)
            ? DVC_INGESTION_ERROR_OTHER
            : executionStatus);
  }

  @Test(dataProvider = "DvcErrorExecutionStatus")
  public void testGetDaVinciErrorStatusNested(ExecutionStatus executionStatus) {
    VeniceException veniceException;
    switch (executionStatus) {
      case DVC_INGESTION_ERROR_DISK_FULL:
        veniceException = new VeniceException(new DiskLimitExhaustedException("test"));
        break;
      case DVC_INGESTION_ERROR_MEMORY_LIMIT_REACHED:
        veniceException = new VeniceException(new MemoryLimitExhaustedException("test"));
        break;
      case DVC_INGESTION_ERROR_TOO_MANY_DEAD_INSTANCES:
      case DVC_INGESTION_ERROR_OTHER:
        veniceException = new VeniceException("test");
        break;
      default:
        fail("Unexpected execution status: " + executionStatus);
        return;
    }
    assertEquals(
        DaVinciBackend.getDaVinciErrorStatus(veniceException),
        executionStatus.equals(ExecutionStatus.DVC_INGESTION_ERROR_TOO_MANY_DEAD_INSTANCES)
            ? DVC_INGESTION_ERROR_OTHER
            : executionStatus);
  }

  @Test(dataProvider = "DvcErrorExecutionStatus")
  public void testGetDaVinciErrorStatusWithInvalidCases(ExecutionStatus executionStatus) {
    VeniceException veniceException;
    switch (executionStatus) {
      case DVC_INGESTION_ERROR_DISK_FULL:
      case DVC_INGESTION_ERROR_MEMORY_LIMIT_REACHED:
      case DVC_INGESTION_ERROR_TOO_MANY_DEAD_INSTANCES:
      case DVC_INGESTION_ERROR_OTHER:
        veniceException = new VeniceException("test");
        break;
      default:
        fail("Unexpected execution status: " + executionStatus);
        return;
    }

    assertEquals(DaVinciBackend.getDaVinciErrorStatus(veniceException), DVC_INGESTION_ERROR_OTHER);
  }

}
