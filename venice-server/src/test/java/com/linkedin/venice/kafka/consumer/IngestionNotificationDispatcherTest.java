package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceIngestionTaskKilledException;
import com.linkedin.venice.notifier.VeniceNotifier;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Queue;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.testng.annotations.Test;


public class IngestionNotificationDispatcherTest {
  /**
   * Mock the case that kill a job of a hybrid store.
   **/
  @Test
  public void testReportKilled() {
    String topic = "test_v1";
    int partitionId = 1;
    VeniceIngestionTaskKilledException error = new VeniceIngestionTaskKilledException("test");
    VeniceNotifier mockNotifier = Mockito.mock(VeniceNotifier.class);
    Queue<VeniceNotifier> notifiers = new ArrayDeque<>();
    notifiers.add(mockNotifier);
    IngestionNotificationDispatcher dispatcher = new IngestionNotificationDispatcher(notifiers, topic, () -> true);
    PartitionConsumptionState psc = Mockito.mock(PartitionConsumptionState.class);

    // Mock a hybrid partition already received the end of push.
    Mockito.doReturn(partitionId).when(psc).getPartition();
    Mockito.doReturn(true).when(psc).isHybrid();
    Mockito.doReturn(true).when(psc).isEndOfPushReceived();
    Mockito.doReturn(false).when(psc).isErrorReported();
    dispatcher.reportKilled(Arrays.asList(new PartitionConsumptionState[]{psc}), error);
    // Should report kill for a hybrid partition
    Mockito.verify(mockNotifier, Mockito.times(1)).error(topic, partitionId, error.getMessage(), error);

    // Mock a batch partition
    Mockito.reset(mockNotifier);
    Mockito.doReturn(false).when(psc).isHybrid();
    Mockito.doReturn(true).when(psc).isCompletionReported();
    dispatcher.reportKilled(Arrays.asList(new PartitionConsumptionState[]{psc}), error);
    // Should not report error for a completed batch partition
    Mockito.verify(mockNotifier, Mockito.never()).error(topic, partitionId, error.getMessage(), error);

    // Error has already been reported
    Mockito.reset(mockNotifier);
    Mockito.doReturn(true).when(psc).isErrorReported();
    dispatcher.reportKilled(Arrays.asList(new PartitionConsumptionState[]{psc}), error);
    Mockito.verify(mockNotifier, Mockito.never()).error(topic, partitionId, error.getMessage(), error);
  }
}
