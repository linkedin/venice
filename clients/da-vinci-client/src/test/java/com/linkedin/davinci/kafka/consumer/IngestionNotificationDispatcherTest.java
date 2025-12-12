package com.linkedin.davinci.kafka.consumer;

import static org.mockito.Mockito.mock;

import com.linkedin.davinci.notifier.VeniceNotifier;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceIngestionTaskKilledException;
import com.linkedin.venice.utils.Utils;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Queue;
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
    VeniceNotifier mockNotifier = mock(VeniceNotifier.class);
    Queue<VeniceNotifier> notifiers = new ArrayDeque<>();
    notifiers.add(mockNotifier);
    IngestionNotificationDispatcher dispatcher =
        new IngestionNotificationDispatcher(notifiers, topic, () -> true, pcs -> 0);
    PartitionConsumptionState psc = mock(PartitionConsumptionState.class);

    // Mock a hybrid partition already received the end of push.
    Mockito.doReturn(partitionId).when(psc).getPartition();
    Mockito.doReturn(partitionId).when(psc).getPartition();
    Mockito.doReturn(true).when(psc).isHybrid();
    Mockito.doReturn(true).when(psc).isEndOfPushReceived();
    Mockito.doReturn(false).when(psc).isErrorReported();
    dispatcher.reportKilled(Collections.singletonList(psc), error);
    // Should report kill for a hybrid partition
    Mockito.verify(mockNotifier, Mockito.times(1)).error(topic, partitionId, error.getMessage(), error);

    // Mock a batch partition
    Mockito.reset(mockNotifier);
    Mockito.doReturn(false).when(psc).isHybrid();
    Mockito.doReturn(true).when(psc).isCompletionReported();
    dispatcher.reportKilled(Collections.singletonList(psc), error);
    // Should not report error for a completed batch partition
    Mockito.verify(mockNotifier, Mockito.never()).error(topic, partitionId, error.getMessage(), error);

    // Error has already been reported
    Mockito.reset(mockNotifier);
    Mockito.doReturn(true).when(psc).isErrorReported();
    dispatcher.reportKilled(Collections.singletonList(psc), error);
    Mockito.verify(mockNotifier, Mockito.never()).error(topic, partitionId, error.getMessage(), error);
  }

  @Test
  public void testReportError() {
    String topic = Utils.getUniqueString("test_v1");
    int partitionId = 1;
    VeniceNotifier mockNotifier = mock(VeniceNotifier.class);
    Queue<VeniceNotifier> notifiers = new ArrayDeque<>();
    notifiers.add(mockNotifier);
    IngestionNotificationDispatcher dispatcher =
        new IngestionNotificationDispatcher(notifiers, topic, () -> true, pcs -> 0);
    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    Mockito.doReturn(partitionId).when(pcs).getPartition();
    Mockito.doReturn(true).when(pcs).isHybrid();
    Mockito.doReturn(true).when(pcs).isEndOfPushReceived();
    Mockito.doReturn(false).when(pcs).isErrorReported();
    Mockito.doReturn(false).when(pcs).isComplete();
    dispatcher.reportError(Collections.singletonList(pcs), "fake ingestion error", mock(VeniceException.class));
  }

  @Test
  public void testReportErrorOnBatchError() {
    String topic = Utils.getUniqueString("test_v1");
    int partitionId = 1;
    VeniceNotifier mockNotifier = mock(VeniceNotifier.class);
    Queue<VeniceNotifier> notifiers = new ArrayDeque<>();
    notifiers.add(mockNotifier);
    IngestionNotificationDispatcher dispatcher =
        new IngestionNotificationDispatcher(notifiers, topic, () -> true, pcs -> 0);
    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    Mockito.doReturn(partitionId).when(pcs).getPartition();
    Mockito.doReturn(false).when(pcs).isHybrid();
    Mockito.doReturn(true).when(pcs).isEndOfPushReceived();
    Mockito.doReturn(false).when(pcs).isErrorReported();
    Mockito.doReturn(true).when(pcs).isComplete();
    Mockito.doReturn(false).when(pcs).isCurrentVersion();
    dispatcher.reportError(Collections.singletonList(pcs), "fake ingestion error", mock(VeniceException.class));
  }
}
