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
  @Test
  public void testPushStatusCollector() {
    ReadWriteStoreRepository storeRepository = mock(ReadWriteStoreRepository.class);
    PushStatusStoreReader pushStatusStoreReader = mock(PushStatusStoreReader.class);

    String daVinciStoreName = "daVinciStore";
    String daVinciStoreTopicV1 = "daVinciStore_v1";
    String daVinciStoreTopicV2 = "daVinciStore_v2";
    String daVinciStoreTopicV3 = "daVinciStore_v3";
    String daVinciStoreTopicV4 = "daVinciStore_v4";
    String daVinciStoreTopicV5 = "daVinciStore_v5";
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
    BiConsumer<String, String> pushErrorConsumer = (x, y) -> pushErrorCount.getAndIncrement();
    PushStatusCollector pushStatusCollector = new PushStatusCollector(
        storeRepository,
        pushStatusStoreReader,
        pushCompleteConsumer,
        pushErrorConsumer,
        true,
        1,
        4,
        1,
        20);
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
    Map<CharSequence, Integer> successfulInstancePushStatus = Collections.singletonMap("instance", 10);
    Map<CharSequence, Integer> errorInstancePushStatus = Collections.singletonMap("instance", 12);
    Map<CharSequence, Integer> startedInstancePushStatus = Collections.singletonMap("instance", 2);

    when(pushStatusStoreReader.getPartitionStatus(daVinciStoreName, 2, 0, Optional.empty()))
        .thenReturn(startedInstancePushStatus, successfulInstancePushStatus);
    when(pushStatusStoreReader.getPartitionStatus(daVinciStoreName, 3, 0, Optional.empty()))
        .thenReturn(startedInstancePushStatus, successfulInstancePushStatus);
    when(pushStatusStoreReader.getPartitionStatus(daVinciStoreName, 4, 0, Optional.empty()))
        .thenReturn(startedInstancePushStatus, errorInstancePushStatus);
    when(pushStatusStoreReader.getPartitionStatus(daVinciStoreName, 5, 0, Optional.empty()))
        .thenReturn(startedInstancePushStatus, errorInstancePushStatus);
    when(pushStatusStoreReader.isInstanceAlive(daVinciStoreName, "instance")).thenReturn(true);
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

    // Da Vinci Topic v4, DVC ERROR, Server success
    pushCompletedCount.set(0);
    pushErrorCount.set(0);
    pushStatusCollector.subscribeTopic(daVinciStoreTopicV4, 1);
    Assert.assertTrue(pushStatusCollector.getTopicToPushStatusMap().containsKey(daVinciStoreTopicV4));
    TestUtils.waitForNonDeterministicAssertion(
        2,
        TimeUnit.SECONDS,
        true,
        () -> verify(pushStatusStoreReader, atLeast(1)).getPartitionStatus(daVinciStoreName, 4, 0, Optional.empty()));
    Assert.assertEquals(pushErrorCount.get(), 0);
    pushStatusCollector.handleServerPushStatusUpdate(daVinciStoreTopicV4, ExecutionStatus.COMPLETED, null);
    TestUtils.waitForNonDeterministicAssertion(
        2,
        TimeUnit.SECONDS,
        true,
        () -> Assert.assertEquals(pushErrorCount.get(), 1));

    // Da Vinci Topic v5, DVC ERROR, Server ERROR
    pushCompletedCount.set(0);
    pushErrorCount.set(0);
    pushStatusCollector.subscribeTopic(daVinciStoreTopicV5, 1);
    Assert.assertTrue(pushStatusCollector.getTopicToPushStatusMap().containsKey(daVinciStoreTopicV5));
    TestUtils.waitForNonDeterministicAssertion(
        2,
        TimeUnit.SECONDS,
        true,
        () -> verify(pushStatusStoreReader, atLeast(1)).getPartitionStatus(daVinciStoreName, 5, 0, Optional.empty()));
    Assert.assertEquals(pushErrorCount.get(), 0);
    pushStatusCollector.handleServerPushStatusUpdate(daVinciStoreTopicV5, ExecutionStatus.ERROR, null);
    TestUtils.waitForNonDeterministicAssertion(
        2,
        TimeUnit.SECONDS,
        true,
        () -> Assert.assertEquals(pushErrorCount.get(), 1));
  }

  @Test
  public void testPushStatusCollectorDaVinciStatusPollingRetry() {
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
    BiConsumer<String, String> pushErrorConsumer = (x, y) -> pushErrorCount.getAndIncrement();
    PushStatusCollector pushStatusCollector = new PushStatusCollector(
        storeRepository,
        pushStatusStoreReader,
        pushCompleteConsumer,
        pushErrorConsumer,
        true,
        1,
        4,
        1,
        20);
    pushStatusCollector.start();

    pushCompletedCount.set(0);
    pushErrorCount.set(0);
    Map<CharSequence, Integer> successfulInstancePushStatus = Collections.singletonMap("instance", 10);
    Map<CharSequence, Integer> errorInstancePushStatus = Collections.singletonMap("instance", 12);
    Map<CharSequence, Integer> startedInstancePushStatus = Collections.singletonMap("instance", 2);

    when(pushStatusStoreReader.getPartitionStatus(daVinciStoreName, 2, 0, Optional.empty()))
        .thenReturn(Collections.emptyMap(), startedInstancePushStatus, successfulInstancePushStatus);
    when(pushStatusStoreReader.getPartitionStatus(daVinciStoreName, 3, 0, Optional.empty()))
        .thenReturn(Collections.emptyMap(), startedInstancePushStatus, errorInstancePushStatus);
    when(pushStatusStoreReader.isInstanceAlive(daVinciStoreName, "instance")).thenReturn(true);
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

    // Da Vinci Topic v3, DVC ERROR, Server COMPLETED
    pushCompletedCount.set(0);
    pushErrorCount.set(0);
    pushStatusCollector.subscribeTopic(daVinciStoreTopicV3, 1);
    Assert.assertTrue(pushStatusCollector.getTopicToPushStatusMap().containsKey(daVinciStoreTopicV3));
    TestUtils.waitForNonDeterministicAssertion(
        5,
        TimeUnit.SECONDS,
        false,
        () -> verify(pushStatusStoreReader, times(3)).getPartitionStatus(daVinciStoreName, 3, 0, Optional.empty()));
    Assert.assertEquals(pushErrorCount.get(), 0);
    pushStatusCollector.handleServerPushStatusUpdate(daVinciStoreTopicV3, ExecutionStatus.COMPLETED, null);
    TestUtils.waitForNonDeterministicAssertion(
        2,
        TimeUnit.SECONDS,
        false,
        () -> Assert.assertEquals(pushErrorCount.get(), 1));
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
    BiConsumer<String, String> pushErrorConsumer = (x, y) -> pushErrorCount.getAndIncrement();
    PushStatusCollector pushStatusCollector = new PushStatusCollector(
        storeRepository,
        pushStatusStoreReader,
        pushCompleteConsumer,
        pushErrorConsumer,
        true,
        1,
        4,
        0,
        20);
    pushStatusCollector.start();

    pushCompletedCount.set(0);
    pushErrorCount.set(0);
    Map<CharSequence, Integer> successfulInstancePushStatus = Collections.singletonMap("instance", 10);
    Map<CharSequence, Integer> startedInstancePushStatus = Collections.singletonMap("instance", 2);

    when(pushStatusStoreReader.getPartitionStatus(daVinciStoreName, 2, 0, Optional.empty()))
        .thenReturn(Collections.emptyMap());
    when(pushStatusStoreReader.isInstanceAlive(daVinciStoreName, "instance")).thenReturn(true);
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
