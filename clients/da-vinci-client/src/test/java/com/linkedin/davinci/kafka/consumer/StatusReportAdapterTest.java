package com.linkedin.davinci.kafka.consumer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.davinci.notifier.VeniceNotifier;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class StatusReportAdapterTest {
  private static final Logger LOGGER = LogManager.getLogger(StatusReportAdapterTest.class);
  private final List<ExecutionStatus> recordStatusList = new ArrayList<>();
  private final Random random = new Random();

  @Test
  public void testStatusReportWithAmpFactor() {
    recordStatusList.clear();
    String topic = "test_v1";
    int amplificationFactor = 3;
    VeniceNotifier notifier = getNotifier();
    Queue<VeniceNotifier> notifiers = new ArrayDeque<>();
    notifiers.add(notifier);
    IngestionNotificationDispatcher dispatcher = new IngestionNotificationDispatcher(notifiers, topic, () -> true);
    ConcurrentMap<Integer, PartitionConsumptionState> partitionConsumptionStateMap =
        generateMockedPcsMap(amplificationFactor);
    AmplificationFactorAdapter amplificationFactorAdapter = mock(AmplificationFactorAdapter.class);
    when(amplificationFactorAdapter.getAmplificationFactor()).thenReturn(amplificationFactor);
    StatusReportAdapter statusReportAdapter = new StatusReportAdapter(dispatcher, amplificationFactorAdapter);
    statusReportAdapter.initializePartitionReportStatus(0);

    ExecutorService executorService = Executors.newFixedThreadPool(amplificationFactor);
    List<Callable<Void>> callableList = new ArrayList<>();
    List<ExecutionStatus> executionStatusList = new ArrayList<>();
    executionStatusList.add(ExecutionStatus.STARTED);
    executionStatusList.add(ExecutionStatus.END_OF_PUSH_RECEIVED);
    executionStatusList.add(ExecutionStatus.COMPLETED);
    for (int i = 0; i < amplificationFactor; i++) {
      callableList
          .add(getReportCallable(statusReportAdapter, partitionConsumptionStateMap.get(i), executionStatusList));
    }
    try {
      executorService.invokeAll(callableList, 10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    Assert.assertEquals(recordStatusList, executionStatusList, "Reported status " + recordStatusList);
  }

  private ConcurrentMap<Integer, PartitionConsumptionState> generateMockedPcsMap(int ampFactor) {
    ConcurrentMap<Integer, PartitionConsumptionState> pcsMap = new VeniceConcurrentHashMap<>();
    OffsetRecord mockedOffsetRecord = mock(OffsetRecord.class);
    byte[] dummyByteArray = new byte[0];
    when(mockedOffsetRecord.toBytes()).thenReturn(dummyByteArray);
    for (int i = 0; i < ampFactor; i++) {
      PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
      // Mock pcs APIs
      Mockito.doReturn(i).when(pcs).getPartition();
      Mockito.doReturn(0).when(pcs).getUserPartition();
      Mockito.doReturn(LeaderFollowerStateType.STANDBY).when(pcs).getLeaderFollowerState();
      Mockito.doReturn(true).when(pcs).isComplete();
      Mockito.doReturn(mockedOffsetRecord).when(pcs).getOffsetRecord();
      pcsMap.put(i, pcs);
    }
    return pcsMap;
  }

  private Callable<Void> getReportCallable(
      StatusReportAdapter adapter,
      PartitionConsumptionState pcs,
      List<ExecutionStatus> executionStatusList) {
    return () -> {
      try {
        for (ExecutionStatus executionStatus: executionStatusList) {
          Thread.sleep(random.nextInt(100));
          LOGGER.info("Sending report status: {} to partition {}", executionStatus, pcs.getPartition());
          switch (executionStatus) {
            case STARTED:
              adapter.reportStarted(pcs);
              break;
            case END_OF_PUSH_RECEIVED:
              adapter.reportEndOfPushReceived(pcs);
              break;
            case COMPLETED:
              adapter.reportCompleted(pcs);
              break;
            default:
              throw new VeniceException("Unsupported execution status: " + executionStatus);
          }
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return null;
    };
  }

  private VeniceNotifier getNotifier() {
    return new VeniceNotifier() {
      @Override
      public void started(String kafkaTopic, int partitionId) {
        recordStatus(ExecutionStatus.STARTED);
      }

      @Override
      public void endOfPushReceived(String kafkaTopic, int partitionId, long offset) {
        recordStatus(ExecutionStatus.END_OF_PUSH_RECEIVED);
      }

      @Override
      public void completed(String kafkaTopic, int partitionId, long offset, String message) {
        recordStatus(ExecutionStatus.COMPLETED);
      }
    };

  }

  private void recordStatus(ExecutionStatus status) {
    recordStatusList.add(status);
    LOGGER.info("Write push status: {} to mocked push monitor.", status);
  }
}
