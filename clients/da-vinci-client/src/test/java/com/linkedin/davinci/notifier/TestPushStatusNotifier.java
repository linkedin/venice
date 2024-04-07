package com.linkedin.davinci.notifier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import com.linkedin.venice.helix.HelixPartitionStatusAccessor;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushmonitor.OfflinePushAccessor;
import com.linkedin.venice.pushstatushelper.PushStatusStoreWriter;
import org.apache.helix.HelixException;
import org.testng.annotations.Test;


public class TestPushStatusNotifier {
  @Test
  public void testCompleteCVUpdate() {
    OfflinePushAccessor offlinePushAccessor = mock(OfflinePushAccessor.class);
    HelixPartitionStatusAccessor helixPartitionStatusAccessor = mock(HelixPartitionStatusAccessor.class);
    PushStatusStoreWriter pushStatusStoreWriter = mock(PushStatusStoreWriter.class);
    ReadOnlyStoreRepository storeRepository = mock(ReadOnlyStoreRepository.class);
    String topic = "abc_v1";
    String host = "localhost";

    PushStatusNotifier statusNotifier = new PushStatusNotifier(
        offlinePushAccessor,
        helixPartitionStatusAccessor,
        pushStatusStoreWriter,
        storeRepository,
        host);
    statusNotifier.completed(topic, 1, 1, "");
    verify(offlinePushAccessor, times(1)).updateReplicaStatus(topic, 1, host, ExecutionStatus.COMPLETED, 1, "");

    doThrow(HelixException.class).when(helixPartitionStatusAccessor)
        .updateReplicaStatus(any(), anyInt(), eq(ExecutionStatus.COMPLETED));
    statusNotifier.completed(topic, 1, 1, "");
    verify(offlinePushAccessor, never()).updateReplicaStatus(topic, 1, "host", ExecutionStatus.COMPLETED, 1, "");

    doReturn(mock(Store.class)).when(storeRepository).getStoreOrThrow(any());
    statusNotifier.startOfIncrementalPushReceived(topic, 1, 1, "");
    statusNotifier.endOfIncrementalPushReceived(topic, 1, 1, "");
    statusNotifier.quotaNotViolated(topic, 1, 1, "");
    statusNotifier.quotaViolated(topic, 1, 1, "");
    statusNotifier.restarted(topic, 1, 1, "");
    statusNotifier.topicSwitchReceived(topic, 1, 1, "");
    statusNotifier.started(topic, 1, "");
    statusNotifier.endOfPushReceived(topic, 1, 1, "");
    statusNotifier.progress(topic, 1, 1, "");
  }
}
