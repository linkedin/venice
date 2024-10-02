package com.linkedin.venice.pushmonitor;

import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.pushstatushelper.PushStatusStoreReader;
import com.linkedin.venice.utils.TestUtils;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.testng.Assert;
import org.testng.annotations.Test;


public class PushStatusCollectorTest {
  // DVC status
  Map<CharSequence, Integer> startedInstancePushStatus =
      Collections.singletonMap("instance", ExecutionStatus.STARTED.getValue());
  Map<CharSequence, Integer> successfulInstancePushStatus =
      Collections.singletonMap("instance", ExecutionStatus.COMPLETED.getValue());
  Map<CharSequence, Integer> dvcDiskErrorInstancePushStatus =
      Collections.singletonMap("instance", ExecutionStatus.DVC_INGESTION_ERROR_DISK_FULL.getValue());
  Map<CharSequence, Integer> dvcMemoryLimitErrorInstancePushStatus =
      Collections.singletonMap("instance", ExecutionStatus.DVC_INGESTION_ERROR_MEMORY_LIMIT_REACHED.getValue());
  Map<CharSequence, Integer> dvcTooManyDeadInstancesErrorInstancePushStatus =
      Collections.singletonMap("instance", ExecutionStatus.DVC_INGESTION_ERROR_TOO_MANY_DEAD_INSTANCES.getValue());
  Map<CharSequence, Integer> dvcOtherErrorInstancePushStatus =
      Collections.singletonMap("instance", ExecutionStatus.DVC_INGESTION_ERROR_OTHER.getValue());

  @Test
  public void testPushStatusCollector() {
    ReadWriteStoreRepository storeRepository = mock(ReadWriteStoreRepository.class);
    PushStatusStoreReader pushStatusStoreReader = mock(PushStatusStoreReader.class);

    String daVinciStoreName = "daVinciStore";
    String daVinciStoreTopicV1 = "daVinciStore_v1";
    String daVinciStoreTopicV2 = "daVinciStore_v2";
    String daVinciStoreTopicV3 = "daVinciStore_v3";

    Store daVinciStore = mock(Store.class);
    when(daVinciStore.isDaVinciPushStatusStoreEnabled()).thenReturn(true);
    when(storeRepository.getStore(daVinciStoreName)).thenReturn(daVinciStore);

    String regularStoreName = "regularStore";
    String regularStoreTopicV1 = "regularStore_v1";
    String regularStoreTopicV2 = "regularStore_v2";
    Store regularStore = mock(Store.class);
    when(regularStore.isDaVinciPushStatusStoreEnabled()).thenReturn(false);
    when(storeRepository.getStore(regularStoreName)).thenReturn(regularStore);

    AtomicInteger pushCompletedCount = new AtomicInteger();
    AtomicInteger pushErrorCount = new AtomicInteger();

    Consumer<String> pushCompleteConsumer = x -> pushCompletedCount.getAndIncrement();
    BiConsumer<String, ExecutionStatusWithDetails> pushErrorConsumer = (x, y) -> pushErrorCount.getAndIncrement();
    PushStatusCollector pushStatusCollector = new PushStatusCollector(
        storeRepository,
        pushStatusStoreReader,
        pushCompleteConsumer,
        pushErrorConsumer,
        true,
        1,
        4,
        1,
        20,
        1,
        true);
    pushStatusCollector.start();

    pushStatusCollector.subscribeTopic(regularStoreTopicV1, 10);
    Assert.assertFalse(pushStatusCollector.getTopicToPushStatusMap().containsKey(regularStoreTopicV1));
    pushStatusCollector.handleServerPushStatusUpdate(regularStoreTopicV1, ExecutionStatus.COMPLETED, null);
    Assert.assertEquals(pushCompletedCount.get(), 1);

    pushStatusCollector.subscribeTopic(regularStoreTopicV2, 10);
    Assert.assertFalse(pushStatusCollector.getTopicToPushStatusMap().containsKey(regularStoreTopicV2));
    pushStatusCollector.handleServerPushStatusUpdate(regularStoreTopicV2, ExecutionStatus.ERROR, "ERROR!!!");
    Assert.assertEquals(pushErrorCount.get(), 1);

    pushCompletedCount.set(0);
    pushErrorCount.set(0);

    when(pushStatusStoreReader.getPartitionStatus(daVinciStoreName, 2, 0, Optional.empty()))
        .thenReturn(startedInstancePushStatus, successfulInstancePushStatus);
    when(pushStatusStoreReader.getPartitionStatus(daVinciStoreName, 3, 0, Optional.empty()))
        .thenReturn(startedInstancePushStatus, successfulInstancePushStatus);
    when(pushStatusStoreReader.getPartitionStatus(daVinciStoreName, 4, 0, Optional.empty()))
        .thenReturn(startedInstancePushStatus, dvcDiskErrorInstancePushStatus);
    when(pushStatusStoreReader.getPartitionStatus(daVinciStoreName, 5, 0, Optional.empty()))
        .thenReturn(startedInstancePushStatus, dvcMemoryLimitErrorInstancePushStatus);
    when(pushStatusStoreReader.getPartitionStatus(daVinciStoreName, 6, 0, Optional.empty()))
        .thenReturn(startedInstancePushStatus, dvcTooManyDeadInstancesErrorInstancePushStatus);
    when(pushStatusStoreReader.getPartitionStatus(daVinciStoreName, 7, 0, Optional.empty()))
        .thenReturn(startedInstancePushStatus, dvcOtherErrorInstancePushStatus);
    when(pushStatusStoreReader.getPartitionStatus(daVinciStoreName, 8, 0, Optional.empty()))
        .thenReturn(startedInstancePushStatus, dvcDiskErrorInstancePushStatus);
    when(pushStatusStoreReader.getPartitionStatus(daVinciStoreName, 9, 0, Optional.empty()))
        .thenReturn(startedInstancePushStatus, dvcMemoryLimitErrorInstancePushStatus);
    when(pushStatusStoreReader.getPartitionStatus(daVinciStoreName, 10, 0, Optional.empty()))
        .thenReturn(startedInstancePushStatus, dvcTooManyDeadInstancesErrorInstancePushStatus);
    when(pushStatusStoreReader.getPartitionStatus(daVinciStoreName, 11, 0, Optional.empty()))
        .thenReturn(startedInstancePushStatus, dvcOtherErrorInstancePushStatus);
    when(pushStatusStoreReader.getInstanceStatus(daVinciStoreName, "instance"))
        .thenReturn(PushStatusStoreReader.InstanceStatus.ALIVE);
    pushStatusCollector.subscribeTopic(daVinciStoreTopicV1, 1);
    Assert.assertFalse(pushStatusCollector.getTopicToPushStatusMap().containsKey(daVinciStoreTopicV1));

    // Da Vinci Topic v2, DVC success, Server success
    pushCompletedCount.set(0);
    pushErrorCount.set(0);
    pushStatusCollector.subscribeTopic(daVinciStoreTopicV2, 1);
    Assert.assertTrue(pushStatusCollector.getTopicToPushStatusMap().containsKey(daVinciStoreTopicV2));
    TestUtils.waitForNonDeterministicAssertion(
        2,
        TimeUnit.SECONDS,
        true,
        () -> verify(pushStatusStoreReader, atLeast(1)).getPartitionStatus(daVinciStoreName, 2, 0, Optional.empty()));
    Assert.assertEquals(pushCompletedCount.get(), 0);
    pushStatusCollector.handleServerPushStatusUpdate(daVinciStoreTopicV2, ExecutionStatus.COMPLETED, null);
    TestUtils.waitForNonDeterministicAssertion(
        2,
        TimeUnit.SECONDS,
        true,
        () -> Assert.assertEquals(pushCompletedCount.get(), 1));

    // Da Vinci Topic v3, DVC success, Server ERROR
    pushCompletedCount.set(0);
    pushErrorCount.set(0);
    pushStatusCollector.subscribeTopic(daVinciStoreTopicV3, 1);
    Assert.assertTrue(pushStatusCollector.getTopicToPushStatusMap().containsKey(daVinciStoreTopicV3));
    TestUtils.waitForNonDeterministicAssertion(
        2,
        TimeUnit.SECONDS,
        true,
        () -> verify(pushStatusStoreReader, atLeast(1)).getPartitionStatus(daVinciStoreName, 3, 0, Optional.empty()));
    Assert.assertEquals(pushErrorCount.get(), 0);
    pushStatusCollector.handleServerPushStatusUpdate(daVinciStoreTopicV3, ExecutionStatus.ERROR, "ERROR!!!!");
    TestUtils.waitForNonDeterministicAssertion(
        2,
        TimeUnit.SECONDS,
        true,
        () -> Assert.assertEquals(pushErrorCount.get(), 1));

    // Da Vinci Topic v4 to v7, DVC ERROR, Server success
    for (int version = 4; version <= 7; version++) {
      String partition = "daVinciStore_v" + version;
      pushCompletedCount.set(0);
      pushErrorCount.set(0);
      pushStatusCollector.subscribeTopic(partition, 1);
      Assert.assertTrue(pushStatusCollector.getTopicToPushStatusMap().containsKey(partition));
      int finalVersion = version;
      TestUtils.waitForNonDeterministicAssertion(
          2,
          TimeUnit.SECONDS,
          true,
          () -> verify(pushStatusStoreReader, atLeast(1))
              .getPartitionStatus(daVinciStoreName, finalVersion, 0, Optional.empty()));
      Assert.assertEquals(pushErrorCount.get(), 0);
      pushStatusCollector.handleServerPushStatusUpdate(partition, ExecutionStatus.COMPLETED, null);
      TestUtils.waitForNonDeterministicAssertion(
          2,
          TimeUnit.SECONDS,
          true,
          () -> Assert.assertEquals(pushErrorCount.get(), 1));
    }

    // Da Vinci Topic v8 to v11, DVC ERROR, Server ERROR
    for (int version = 8; version <= 11; version++) {
      String partition = "daVinciStore_v" + version;
      pushCompletedCount.set(0);
      pushErrorCount.set(0);
      pushStatusCollector.subscribeTopic(partition, 1);
      Assert.assertTrue(pushStatusCollector.getTopicToPushStatusMap().containsKey(partition));
      int finalVersion = version;
      TestUtils.waitForNonDeterministicAssertion(
          2,
          TimeUnit.SECONDS,
          true,
          () -> verify(pushStatusStoreReader, atLeast(1))
              .getPartitionStatus(daVinciStoreName, finalVersion, 0, Optional.empty()));
      Assert.assertEquals(pushErrorCount.get(), 0);
      pushStatusCollector.handleServerPushStatusUpdate(partition, ExecutionStatus.ERROR, null);
      TestUtils.waitForNonDeterministicAssertion(
          2,
          TimeUnit.SECONDS,
          true,
          () -> Assert.assertEquals(pushErrorCount.get(), 1));
    }
  }

  @Test
  public void testPushStatusCollectorDaVinciStatusPollingRetry() {
    ReadWriteStoreRepository storeRepository = mock(ReadWriteStoreRepository.class);
    PushStatusStoreReader pushStatusStoreReader = mock(PushStatusStoreReader.class);

    String daVinciStoreName = "daVinciStore";
    String daVinciStoreTopicV1 = "daVinciStore_v1";
    String daVinciStoreTopicV2 = "daVinciStore_v2";

    Store daVinciStore = mock(Store.class);
    when(daVinciStore.isDaVinciPushStatusStoreEnabled()).thenReturn(true);
    when(storeRepository.getStore(daVinciStoreName)).thenReturn(daVinciStore);

    AtomicInteger pushCompletedCount = new AtomicInteger();
    AtomicInteger pushErrorCount = new AtomicInteger();

    Consumer<String> pushCompleteConsumer = x -> pushCompletedCount.getAndIncrement();
    BiConsumer<String, ExecutionStatusWithDetails> pushErrorConsumer = (x, y) -> pushErrorCount.getAndIncrement();
    PushStatusCollector pushStatusCollector = new PushStatusCollector(
        storeRepository,
        pushStatusStoreReader,
        pushCompleteConsumer,
        pushErrorConsumer,
        true,
        1,
        4,
        1,
        20,
        1,
        true);
    pushStatusCollector.start();

    pushCompletedCount.set(0);
    pushErrorCount.set(0);

    when(pushStatusStoreReader.getPartitionStatus(daVinciStoreName, 2, 0, Optional.empty()))
        .thenReturn(Collections.emptyMap(), startedInstancePushStatus, successfulInstancePushStatus);
    when(pushStatusStoreReader.getPartitionStatus(daVinciStoreName, 3, 0, Optional.empty()))
        .thenReturn(Collections.emptyMap(), startedInstancePushStatus, dvcDiskErrorInstancePushStatus);
    when(pushStatusStoreReader.getPartitionStatus(daVinciStoreName, 4, 0, Optional.empty()))
        .thenReturn(Collections.emptyMap(), startedInstancePushStatus, dvcMemoryLimitErrorInstancePushStatus);
    when(pushStatusStoreReader.getPartitionStatus(daVinciStoreName, 5, 0, Optional.empty()))
        .thenReturn(Collections.emptyMap(), startedInstancePushStatus, dvcTooManyDeadInstancesErrorInstancePushStatus);
    when(pushStatusStoreReader.getPartitionStatus(daVinciStoreName, 6, 0, Optional.empty()))
        .thenReturn(Collections.emptyMap(), startedInstancePushStatus, dvcOtherErrorInstancePushStatus);
    when(pushStatusStoreReader.getInstanceStatus(daVinciStoreName, "instance"))
        .thenReturn(PushStatusStoreReader.InstanceStatus.ALIVE);
    pushStatusCollector.subscribeTopic(daVinciStoreTopicV1, 1);
    Assert.assertFalse(pushStatusCollector.getTopicToPushStatusMap().containsKey(daVinciStoreTopicV1));

    // Da Vinci Topic v2, DVC success, Server success
    pushCompletedCount.set(0);
    pushErrorCount.set(0);
    pushStatusCollector.subscribeTopic(daVinciStoreTopicV2, 1);
    Assert.assertTrue(pushStatusCollector.getTopicToPushStatusMap().containsKey(daVinciStoreTopicV2));
    TestUtils.waitForNonDeterministicAssertion(
        5,
        TimeUnit.SECONDS,
        false,
        () -> verify(pushStatusStoreReader, times(3)).getPartitionStatus(daVinciStoreName, 2, 0, Optional.empty()));
    Assert.assertEquals(pushCompletedCount.get(), 0);
    pushStatusCollector.handleServerPushStatusUpdate(daVinciStoreTopicV2, ExecutionStatus.COMPLETED, null);
    TestUtils.waitForNonDeterministicAssertion(
        2,
        TimeUnit.SECONDS,
        false,
        () -> Assert.assertEquals(pushCompletedCount.get(), 1));

    // Da Vinci Topic v3 to v6, DVC ERROR, Server COMPLETED
    for (int version = 3; version <= 6; version++) {
      String partition = "daVinciStore_v" + version;
      pushCompletedCount.set(0);
      pushErrorCount.set(0);
      pushStatusCollector.subscribeTopic(partition, 1);
      Assert.assertTrue(pushStatusCollector.getTopicToPushStatusMap().containsKey(partition));
      int finalVersion = version;
      TestUtils.waitForNonDeterministicAssertion(
          5,
          TimeUnit.SECONDS,
          false,
          () -> verify(pushStatusStoreReader, times(3))
              .getPartitionStatus(daVinciStoreName, finalVersion, 0, Optional.empty()));
      Assert.assertEquals(pushErrorCount.get(), 0);
      pushStatusCollector.handleServerPushStatusUpdate(partition, ExecutionStatus.COMPLETED, null);
      TestUtils.waitForNonDeterministicAssertion(
          2,
          TimeUnit.SECONDS,
          false,
          () -> Assert.assertEquals(pushErrorCount.get(), 1));
    }
  }

  @Test
  public void testPushStatusCollectorDaVinciStatusPollingRetryWhenEmptyResultUntilServerCompleteOrNonEmptyResult() {
    ReadWriteStoreRepository storeRepository = mock(ReadWriteStoreRepository.class);
    PushStatusStoreReader pushStatusStoreReader = mock(PushStatusStoreReader.class);

    String daVinciStoreName = "daVinciStore";
    String daVinciStoreTopicV1 = "daVinciStore_v1";
    String daVinciStoreTopicV2 = "daVinciStore_v2";
    String daVinciStoreTopicV3 = "daVinciStore_v3";

    Store daVinciStore = mock(Store.class);
    when(daVinciStore.isDaVinciPushStatusStoreEnabled()).thenReturn(true);
    when(storeRepository.getStore(daVinciStoreName)).thenReturn(daVinciStore);

    AtomicInteger pushCompletedCount = new AtomicInteger();
    AtomicInteger pushErrorCount = new AtomicInteger();

    Consumer<String> pushCompleteConsumer = x -> pushCompletedCount.getAndIncrement();
    BiConsumer<String, ExecutionStatusWithDetails> pushErrorConsumer = (x, y) -> pushErrorCount.getAndIncrement();
    PushStatusCollector pushStatusCollector = new PushStatusCollector(
        storeRepository,
        pushStatusStoreReader,
        pushCompleteConsumer,
        pushErrorConsumer,
        true,
        1,
        4,
        0,
        20,
        1,
        true);
    pushStatusCollector.start();

    pushCompletedCount.set(0);
    pushErrorCount.set(0);
    // DVC status
    Map<CharSequence, Integer> startedInstancePushStatus =
        Collections.singletonMap("instance", ExecutionStatus.STARTED.getValue());
    Map<CharSequence, Integer> successfulInstancePushStatus =
        Collections.singletonMap("instance", ExecutionStatus.COMPLETED.getValue());

    when(pushStatusStoreReader.getPartitionStatus(daVinciStoreName, 2, 0, Optional.empty()))
        .thenReturn(Collections.emptyMap());
    when(pushStatusStoreReader.getInstanceStatus(daVinciStoreName, "instance"))
        .thenReturn(PushStatusStoreReader.InstanceStatus.ALIVE);
    pushStatusCollector.subscribeTopic(daVinciStoreTopicV1, 1);
    Assert.assertFalse(pushStatusCollector.getTopicToPushStatusMap().containsKey(daVinciStoreTopicV1));

    // Da Vinci Topic v2, DVC success, Server success
    pushCompletedCount.set(0);
    pushErrorCount.set(0);
    pushStatusCollector.subscribeTopic(daVinciStoreTopicV2, 1);
    Assert.assertTrue(pushStatusCollector.getTopicToPushStatusMap().containsKey(daVinciStoreTopicV2));
    TestUtils.waitForNonDeterministicAssertion(
        5,
        TimeUnit.SECONDS,
        false,
        () -> verify(pushStatusStoreReader, atLeast(3)).getPartitionStatus(daVinciStoreName, 2, 0, Optional.empty()));
    Assert.assertEquals(pushCompletedCount.get(), 0);
    pushStatusCollector.handleServerPushStatusUpdate(daVinciStoreTopicV2, ExecutionStatus.COMPLETED, null);
    TestUtils.waitForNonDeterministicAssertion(
        2,
        TimeUnit.SECONDS,
        false,
        () -> Assert.assertEquals(pushCompletedCount.get(), 1));

    // Da Vinci Topic v3, DVC COMPLETE, Server COMPLETED
    pushCompletedCount.set(0);
    pushErrorCount.set(0);

    when(pushStatusStoreReader.getPartitionStatus(daVinciStoreName, 3, 0, Optional.empty())).thenReturn(
        Collections.emptyMap(),
        Collections.emptyMap(),
        Collections.emptyMap(),
        startedInstancePushStatus,
        successfulInstancePushStatus);

    pushStatusCollector.subscribeTopic(daVinciStoreTopicV3, 1);
    Assert.assertTrue(pushStatusCollector.getTopicToPushStatusMap().containsKey(daVinciStoreTopicV3));
    TestUtils.waitForNonDeterministicAssertion(
        10,
        TimeUnit.SECONDS,
        false,
        () -> verify(pushStatusStoreReader, times(5)).getPartitionStatus(daVinciStoreName, 3, 0, Optional.empty()));
    Assert.assertEquals(pushCompletedCount.get(), 0);
    pushStatusCollector.handleServerPushStatusUpdate(daVinciStoreTopicV3, ExecutionStatus.COMPLETED, null);
    TestUtils.waitForNonDeterministicAssertion(
        2,
        TimeUnit.SECONDS,
        false,
        () -> Assert.assertEquals(pushCompletedCount.get(), 1));
  }
}
