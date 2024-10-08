package com.linkedin.davinci.notifier;

import static com.linkedin.davinci.config.VeniceServerConfig.IncrementalPushStatusWriteMode.DUAL;
import static com.linkedin.davinci.config.VeniceServerConfig.IncrementalPushStatusWriteMode.PUSH_STATUS_SYSTEM_STORE_ONLY;
import static com.linkedin.davinci.config.VeniceServerConfig.IncrementalPushStatusWriteMode.ZOOKEEPER_ONLY;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.START_OF_INCREMENTAL_PUSH_RECEIVED;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.davinci.config.VeniceServerConfig.IncrementalPushStatusWriteMode;
import com.linkedin.venice.helix.HelixPartitionStatusAccessor;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushmonitor.OfflinePushAccessor;
import com.linkedin.venice.pushstatushelper.PushStatusStoreWriter;
import org.apache.helix.HelixException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class TestPushStatusNotifier {
  public static final String INSTANCE_ID = "instance1";
  private OfflinePushAccessor offlinePushAccessor;
  private HelixPartitionStatusAccessor helixPartitionStatusAccessor;
  private PushStatusStoreWriter pushStatusStoreWriter;
  private ReadOnlyStoreRepository storeRepository;
  private PushStatusNotifier notifier;
  private static final String STORE_NAME = "test_store";
  private static final int STORE_VERSION = 1;
  private static final String TOPIC = "test_store_v1";
  private static final int PARTITION_ID = 1;
  private static final long OFFSET = 12345L;
  private static final String MESSAGE = "Test Message";

  @BeforeMethod
  public void setUp() {
    offlinePushAccessor = mock(OfflinePushAccessor.class);
    helixPartitionStatusAccessor = mock(HelixPartitionStatusAccessor.class);
    pushStatusStoreWriter = mock(PushStatusStoreWriter.class);
    storeRepository = mock(ReadOnlyStoreRepository.class);
    Store store = mock(Store.class);
    when(store.isDaVinciPushStatusStoreEnabled()).thenReturn(true);
    when(storeRepository.getStoreOrThrow(any())).thenReturn(store);
  }

  @Test
  public void testCompleteCVUpdate() {
    ReadOnlyStoreRepository storeRepository = mock(ReadOnlyStoreRepository.class);
    String topic = "abc_v1";
    String host = "localhost";

    PushStatusNotifier statusNotifier = new PushStatusNotifier(
        offlinePushAccessor,
        helixPartitionStatusAccessor,
        pushStatusStoreWriter,
        storeRepository,
        host,
        DUAL);
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

  @DataProvider(name = "startOfIncrementalPushCases")
  public Object[][] startOfIncrementalPushCases() {
    return new Object[][] { { ZOOKEEPER_ONLY, true, false }, { PUSH_STATUS_SYSTEM_STORE_ONLY, false, true },
        { DUAL, true, true } };
  }

  @Test(dataProvider = "startOfIncrementalPushCases")
  public void testStartOfIncrementalPushReceived(
      IncrementalPushStatusWriteMode mode,
      boolean expectZookeeper,
      boolean expectPushStatusStore) {

    notifier = new PushStatusNotifier(
        offlinePushAccessor,
        helixPartitionStatusAccessor,
        pushStatusStoreWriter,
        storeRepository,
        "instance1",
        mode);

    notifier.startOfIncrementalPushReceived(TOPIC, PARTITION_ID, OFFSET, MESSAGE);

    if (expectZookeeper) {
      verify(offlinePushAccessor, times(1))
          .updateReplicaStatus(TOPIC, PARTITION_ID, INSTANCE_ID, START_OF_INCREMENTAL_PUSH_RECEIVED, OFFSET, MESSAGE);
    } else {
      verify(offlinePushAccessor, never())
          .updateReplicaStatus(anyString(), anyInt(), anyString(), any(ExecutionStatus.class), anyLong(), anyString());
    }

    if (expectPushStatusStore) {
      verify(pushStatusStoreWriter, times(1)).writePushStatus(
          eq(STORE_NAME),
          eq(STORE_VERSION),
          eq(PARTITION_ID),
          eq(START_OF_INCREMENTAL_PUSH_RECEIVED),
          any(),
          any());
    } else {
      verify(pushStatusStoreWriter, never())
          .writePushStatus(anyString(), anyInt(), anyInt(), any(ExecutionStatus.class), any());
    }
  }

  @DataProvider(name = "endOfIncrementalPushCases")
  public Object[][] endOfIncrementalPushCases() {
    return new Object[][] { { ZOOKEEPER_ONLY, true, false }, { PUSH_STATUS_SYSTEM_STORE_ONLY, false, true },
        { DUAL, true, true } };
  }

  @Test(dataProvider = "endOfIncrementalPushCases")
  public void testEndOfIncrementalPushReceived(
      IncrementalPushStatusWriteMode mode,
      boolean expectZookeeper,
      boolean expectPushStatusStore) {

    notifier = new PushStatusNotifier(
        offlinePushAccessor,
        helixPartitionStatusAccessor,
        pushStatusStoreWriter,
        storeRepository,
        INSTANCE_ID,
        mode);

    notifier.endOfIncrementalPushReceived(TOPIC, PARTITION_ID, OFFSET, MESSAGE);

    if (expectZookeeper) {
      verify(offlinePushAccessor, times(1))
          .updateReplicaStatus(TOPIC, PARTITION_ID, INSTANCE_ID, END_OF_INCREMENTAL_PUSH_RECEIVED, OFFSET, MESSAGE);
    } else {
      verify(offlinePushAccessor, never())
          .updateReplicaStatus(anyString(), anyInt(), anyString(), any(ExecutionStatus.class), anyLong(), anyString());
    }

    if (expectPushStatusStore) {
      verify(pushStatusStoreWriter, times(1)).writePushStatus(
          eq(STORE_NAME),
          eq(STORE_VERSION),
          eq(PARTITION_ID),
          eq(END_OF_INCREMENTAL_PUSH_RECEIVED),
          any(),
          any());
    } else {
      verify(pushStatusStoreWriter, never())
          .writePushStatus(anyString(), anyInt(), anyInt(), any(ExecutionStatus.class), any());
    }
  }
}
