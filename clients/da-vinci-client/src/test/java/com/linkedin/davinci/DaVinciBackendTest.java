package com.linkedin.davinci;

import static com.linkedin.venice.pushmonitor.ExecutionStatus.DVC_INGESTION_ERROR_OTHER;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.ERROR;
import static com.linkedin.venice.utils.DataProviderUtils.BOOLEAN;
import static com.linkedin.venice.utils.DataProviderUtils.allPermutationGenerator;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import com.linkedin.venice.exceptions.DiskLimitExhaustedException;
import com.linkedin.venice.exceptions.MemoryLimitExhaustedException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class DaVinciBackendTest {
  @DataProvider(name = "DvcErrorExecutionStatusAndBoolean")
  public static Object[][] dvcErrorExecutionStatusAndBoolean() {
    return allPermutationGenerator((permutation) -> {
      ExecutionStatus status = (ExecutionStatus) permutation[0];
      return status.isDVCIngestionError();
    }, ExecutionStatus.values(), BOOLEAN);
  }

  @Test(dataProvider = "DvcErrorExecutionStatusAndBoolean")
  public void testGetDaVinciErrorStatus(
      ExecutionStatus executionStatus,
      boolean useDaVinciSpecificExecutionStatusForError) {
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
    if (useDaVinciSpecificExecutionStatusForError) {
      assertEquals(
          DaVinciBackend.getDaVinciErrorStatus(veniceException, useDaVinciSpecificExecutionStatusForError),
          executionStatus.equals(ExecutionStatus.DVC_INGESTION_ERROR_TOO_MANY_DEAD_INSTANCES)
              ? DVC_INGESTION_ERROR_OTHER
              : executionStatus);
    } else {
      assertEquals(
          DaVinciBackend.getDaVinciErrorStatus(veniceException, useDaVinciSpecificExecutionStatusForError),
          ERROR);
    }
  }

  @Test(dataProvider = "DvcErrorExecutionStatusAndBoolean")
  public void testGetDaVinciErrorStatusNested(
      ExecutionStatus executionStatus,
      boolean useDaVinciSpecificExecutionStatusForError) {
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
    if (useDaVinciSpecificExecutionStatusForError) {
      assertEquals(
          DaVinciBackend.getDaVinciErrorStatus(veniceException, useDaVinciSpecificExecutionStatusForError),
          executionStatus.equals(ExecutionStatus.DVC_INGESTION_ERROR_TOO_MANY_DEAD_INSTANCES)
              ? DVC_INGESTION_ERROR_OTHER
              : executionStatus);
    } else {
      assertEquals(
          DaVinciBackend.getDaVinciErrorStatus(veniceException, useDaVinciSpecificExecutionStatusForError),
          ERROR);
    }
  }

  @Test(dataProvider = "DvcErrorExecutionStatusAndBoolean")
  public void testGetDaVinciErrorStatusWithInvalidCases(
      ExecutionStatus executionStatus,
      boolean useDaVinciSpecificExecutionStatusForError) {
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

    if (useDaVinciSpecificExecutionStatusForError) {
      assertEquals(
          DaVinciBackend.getDaVinciErrorStatus(veniceException, useDaVinciSpecificExecutionStatusForError),
          DVC_INGESTION_ERROR_OTHER);

    } else {
      assertEquals(
          DaVinciBackend.getDaVinciErrorStatus(veniceException, useDaVinciSpecificExecutionStatusForError),
          ERROR);
    }
  }

  @Test
  public void testBootstrappingAwareCompletableFuture()
      throws ExecutionException, InterruptedException, TimeoutException {
    DaVinciBackend backend = mock(DaVinciBackend.class);

    when(backend.hasCurrentVersionBootstrapping()).thenReturn(true).thenReturn(false);

    DaVinciBackend.BootstrappingAwareCompletableFuture future =
        new DaVinciBackend.BootstrappingAwareCompletableFuture(backend);
    future.getBootstrappingFuture().get(10, TimeUnit.SECONDS);
    verify(backend, times(2)).hasCurrentVersionBootstrapping();
  }

}
