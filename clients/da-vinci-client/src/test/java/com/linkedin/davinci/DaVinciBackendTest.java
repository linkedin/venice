package com.linkedin.davinci;

import static com.linkedin.venice.pushmonitor.ExecutionStatus.DVC_INGESTION_ERROR_OTHER;
import static com.linkedin.venice.utils.DataProviderUtils.allPermutationGenerator;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.fail;

import com.linkedin.venice.exceptions.MemoryLimitExhaustedException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class DaVinciBackendTest {
  @DataProvider(name = "DvcErrorExecutionStatus")
  public static Object[][] dvcErrorExecutionStatus() {
    return allPermutationGenerator((permutation) -> {
      ExecutionStatus status = (ExecutionStatus) permutation[0];
      if (status.isDVCIngestionError()) {
        return true;
      }
      return false;
    }, ExecutionStatus.values());
  }

  @Test(dataProvider = "DvcErrorExecutionStatus")
  public void testDaVinciReportError(ExecutionStatus executionStatus) {
    DaVinciBackend daVinciBackend = new DaVinciBackend();
    String topic = Utils.getUniqueString("test_v1");
    int partitionId = 1;
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

    DaVinciBackend.DaVinciNotifier ingestionListener = daVinciBackend.getIngestionListener();
    DaVinciBackend.DaVinciNotifier mockIngestionListener = spy(ingestionListener);
    VersionBackend mockVersionBackend = mock(VersionBackend.class);
    daVinciBackend.getVersionByTopicMap().put(topic, mockVersionBackend);
    doAnswer(invocation -> {
      return null;
    }).when(mockIngestionListener).reportPushStatusInDaVinciBackend(anyString(), anyInt(), (ExecutionStatus) any());
    mockIngestionListener.error(topic, partitionId, message, veniceException);
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
      verify(mockIngestionListener, times(1)).reportPushStatusInDaVinciBackend(
          topic,
          partitionId,
          executionStatus.equals(ExecutionStatus.DVC_INGESTION_ERROR_TOO_MANY_DEAD_INSTANCES)
              ? DVC_INGESTION_ERROR_OTHER
              : executionStatus);
    });
  }

  @Test(dataProvider = "DvcErrorExecutionStatus")
  public void testDaVinciReportErrorInvalidCases(ExecutionStatus executionStatus) {
    DaVinciBackend daVinciBackend = new DaVinciBackend();
    String topic = Utils.getUniqueString("test_v1");
    int partitionId = 1;
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

    DaVinciBackend.DaVinciNotifier ingestionListener = daVinciBackend.getIngestionListener();
    DaVinciBackend.DaVinciNotifier mockIngestionListener = spy(ingestionListener);
    VersionBackend mockVersionBackend = mock(VersionBackend.class);
    daVinciBackend.getVersionByTopicMap().put(topic, mockVersionBackend);
    doAnswer(invocation -> {
      return null;
    }).when(mockIngestionListener).reportPushStatusInDaVinciBackend(anyString(), anyInt(), (ExecutionStatus) any());
    mockIngestionListener.error(topic, partitionId, message, veniceException);
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
      verify(mockIngestionListener, times(1))
          .reportPushStatusInDaVinciBackend(topic, partitionId, DVC_INGESTION_ERROR_OTHER);
    });
  }

}
