package com.linkedin.venice.helix;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.venice.acl.VeniceComponent;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushmonitor.OfflinePushStatus;
import com.linkedin.venice.pushmonitor.PartitionStatus;
import com.linkedin.venice.utils.LogContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.helix.AccessOption;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class OfflinePushMonitorAccessorTest {
  @Test
  public void testErrorInGettingOfflinePushStatusCreationTimeIsHandled() {
    ZkBaseDataAccessor<OfflinePushStatus> mockOfflinePushStatusAccessor = mock(ZkBaseDataAccessor.class);
    doThrow(new RuntimeException()).when(mockOfflinePushStatusAccessor).getStat(anyString(), anyInt());
    VeniceOfflinePushMonitorAccessor accessor = new VeniceOfflinePushMonitorAccessor(
        "cluster0",
        mockOfflinePushStatusAccessor,
        mock(ZkBaseDataAccessor.class),
        LogContext.newBuilder()
            .setComponentName(VeniceComponent.CONTROLLER.name())
            .setRegionName("test-region")
            .build());
    Optional<Long> ctime = accessor.getOfflinePushStatusCreationTime("test");
    Assert.assertFalse(ctime.isPresent());
  }

  @Test
  public void testNullStatWillReturnEmptyOptional() {
    ZkBaseDataAccessor<OfflinePushStatus> mockOfflinePushStatusAccessor = mock(ZkBaseDataAccessor.class);
    doReturn(null).when(mockOfflinePushStatusAccessor).getStat(anyString(), anyInt());
    VeniceOfflinePushMonitorAccessor accessor = new VeniceOfflinePushMonitorAccessor(
        "cluster0",
        mockOfflinePushStatusAccessor,
        mock(ZkBaseDataAccessor.class),
        LogContext.newBuilder()
            .setComponentName(VeniceComponent.CONTROLLER.name())
            .setRegionName("test-region")
            .build());
    Optional<Long> ctime = accessor.getOfflinePushStatusCreationTime("test");
    Assert.assertFalse(ctime.isPresent());
  }

  @Test
  public void testBatchUpdateEOIP() {
    ZkBaseDataAccessor<OfflinePushStatus> mockOfflinePushStatusAccessor = mock(ZkBaseDataAccessor.class);
    ZkBaseDataAccessor<PartitionStatus> mockPartitionStatusAccessor = mock(ZkBaseDataAccessor.class);
    doReturn(null).when(mockOfflinePushStatusAccessor).getStat(anyString(), anyInt());
    VeniceOfflinePushMonitorAccessor accessor = new VeniceOfflinePushMonitorAccessor(
        "cluster0",
        mockOfflinePushStatusAccessor,
        mockPartitionStatusAccessor,
        LogContext.newBuilder()
            .setComponentName(VeniceComponent.CONTROLLER.name())
            .setRegionName("test-region")
            .build());
    when(mockPartitionStatusAccessor.exists(anyString(), anyInt())).thenReturn(true);
    when(mockPartitionStatusAccessor.update(anyString(), any(), anyInt())).thenReturn(true);
    accessor.batchUpdateReplicaIncPushStatus("test_topic", 0, "test_instance_id", Arrays.asList("a", "b"));
    Mockito.verify(mockPartitionStatusAccessor, Mockito.times(1)).update(anyString(), any(), anyInt());
  }

  @Test
  public void testLoadOfflinePushStatusesWithLoadThreads() {
    for (int loadThreadCount: new int[] { 1, 4 }) {
      ZkBaseDataAccessor<OfflinePushStatus> mockOfflinePushStatusAccessor = mock(ZkBaseDataAccessor.class);
      ZkBaseDataAccessor<PartitionStatus> mockPartitionStatusAccessor = mock(ZkBaseDataAccessor.class);
      VeniceOfflinePushMonitorAccessor accessor = new VeniceOfflinePushMonitorAccessor(
          "cluster0",
          mockOfflinePushStatusAccessor,
          mockPartitionStatusAccessor,
          getLogContext(),
          loadThreadCount);
      String parentPath = accessor.getOfflinePushStatuesParentPath();
      int pushCount = 10;
      int partitionCount = 3;
      List<OfflinePushStatus> pushStatuses = new ArrayList<>();
      List<String> pushNames = new ArrayList<>();
      for (int i = 0; i < pushCount; i++) {
        String topic = "test_store_v" + i;
        pushStatuses.add(createPushStatus(topic, partitionCount, ExecutionStatus.COMPLETED));
        pushNames.add(topic);
        stubPartitionStatuses(mockPartitionStatusAccessor, parentPath + "/" + topic, topic, partitionCount);
      }
      // A null push status is skipped and a push status that is not a task status is deleted, as before.
      pushStatuses.add(3, null);
      pushNames.add(3, "test_store_null_v1");
      pushStatuses.add(createPushStatus("test_store_invalid_v1", partitionCount, ExecutionStatus.ARCHIVED));
      pushNames.add("test_store_invalid_v1");
      String invalidPushPath = parentPath + "/test_store_invalid_v1";
      doReturn(pushNames).when(mockOfflinePushStatusAccessor).getChildNames(parentPath, AccessOption.PERSISTENT);
      doReturn(pushStatuses).when(mockOfflinePushStatusAccessor).getChildren(parentPath, null, AccessOption.PERSISTENT);
      doReturn(true).when(mockOfflinePushStatusAccessor).remove(invalidPushPath, AccessOption.PERSISTENT);
      Set<String> readThreadNames = ConcurrentHashMap.newKeySet();
      when(mockPartitionStatusAccessor.getChildNames(anyString(), eq(AccessOption.PERSISTENT)))
          .thenAnswer(invocation -> {
            readThreadNames.add(Thread.currentThread().getName());
            return Arrays.asList("0", "1", "2");
          });

      List<OfflinePushStatus> loadedPushes = accessor.loadOfflinePushStatusesAndPartitionStatuses();

      Assert.assertEquals(loadedPushes.size(), pushCount);
      for (int i = 0; i < pushCount; i++) {
        String topic = "test_store_v" + i;
        OfflinePushStatus loadedPush = loadedPushes.get(i);
        Assert.assertEquals(loadedPush.getKafkaTopic(), topic);
        Assert.assertEquals(loadedPush.getPartitionStatuses().size(), partitionCount);
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
          Assert.assertEquals(
              loadedPush.getPartitionStatus(partitionId).getReplicaStatus(topic + "_instance_" + partitionId),
              ExecutionStatus.COMPLETED);
        }
      }
      verify(mockOfflinePushStatusAccessor).remove(invalidPushPath, AccessOption.PERSISTENT);
      verify(mockPartitionStatusAccessor, never()).getChildNames(invalidPushPath, AccessOption.PERSISTENT);
      for (String threadName: readThreadNames) {
        Assert.assertEquals(
            threadName.startsWith("PushStatusLoader-cluster0"),
            loadThreadCount > 1,
            "Unexpected thread " + threadName + " with " + loadThreadCount + " load threads");
      }
    }
  }

  @Test
  public void testLoadOfflinePushStatusesWithLoadThreadsRethrowsReadFailure() {
    ZkBaseDataAccessor<OfflinePushStatus> mockOfflinePushStatusAccessor = mock(ZkBaseDataAccessor.class);
    ZkBaseDataAccessor<PartitionStatus> mockPartitionStatusAccessor = mock(ZkBaseDataAccessor.class);
    VeniceOfflinePushMonitorAccessor accessor = new VeniceOfflinePushMonitorAccessor(
        "cluster0",
        mockOfflinePushStatusAccessor,
        mockPartitionStatusAccessor,
        getLogContext(),
        4);
    String parentPath = accessor.getOfflinePushStatuesParentPath();
    List<OfflinePushStatus> pushStatuses = new ArrayList<>();
    List<String> pushNames = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      String topic = "test_store_v" + i;
      pushStatuses.add(createPushStatus(topic, 1, ExecutionStatus.COMPLETED));
      pushNames.add(topic);
      stubPartitionStatuses(mockPartitionStatusAccessor, parentPath + "/" + topic, topic, 1);
    }
    doReturn(pushNames).when(mockOfflinePushStatusAccessor).getChildNames(parentPath, AccessOption.PERSISTENT);
    doReturn(pushStatuses).when(mockOfflinePushStatusAccessor).getChildren(parentPath, null, AccessOption.PERSISTENT);
    VeniceException readFailure = new VeniceException("Got partial children from zk");
    doThrow(readFailure).when(mockPartitionStatusAccessor)
        .getChildNames(parentPath + "/test_store_v2", AccessOption.PERSISTENT);

    VeniceException thrown =
        Assert.expectThrows(VeniceException.class, accessor::loadOfflinePushStatusesAndPartitionStatuses);
    Assert.assertSame(thrown, readFailure);
  }

  @Test(timeOut = 60 * 1000)
  public void testLoadOfflinePushStatusesWithLoadThreadsStopsWhenInterrupted() throws InterruptedException {
    ZkBaseDataAccessor<OfflinePushStatus> mockOfflinePushStatusAccessor = mock(ZkBaseDataAccessor.class);
    ZkBaseDataAccessor<PartitionStatus> mockPartitionStatusAccessor = mock(ZkBaseDataAccessor.class);
    VeniceOfflinePushMonitorAccessor accessor = new VeniceOfflinePushMonitorAccessor(
        "cluster0",
        mockOfflinePushStatusAccessor,
        mockPartitionStatusAccessor,
        getLogContext(),
        2);
    String parentPath = accessor.getOfflinePushStatuesParentPath();
    List<OfflinePushStatus> pushStatuses = new ArrayList<>();
    List<String> pushNames = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      String topic = "test_store_v" + i;
      pushStatuses.add(createPushStatus(topic, 1, ExecutionStatus.COMPLETED));
      pushNames.add(topic);
    }
    doReturn(pushNames).when(mockOfflinePushStatusAccessor).getChildNames(parentPath, AccessOption.PERSISTENT);
    doReturn(pushStatuses).when(mockOfflinePushStatusAccessor).getChildren(parentPath, null, AccessOption.PERSISTENT);
    // Both partition status reads block until they are interrupted.
    CountDownLatch readsStarted = new CountDownLatch(2);
    CountDownLatch neverReleased = new CountDownLatch(1);
    when(mockPartitionStatusAccessor.getChildNames(anyString(), eq(AccessOption.PERSISTENT))).thenAnswer(invocation -> {
      readsStarted.countDown();
      neverReleased.await();
      return Arrays.asList("0");
    });
    AtomicReference<Throwable> loadFailure = new AtomicReference<>();
    AtomicBoolean interruptedAfterLoad = new AtomicBoolean();
    Thread loader = new Thread(() -> {
      try {
        accessor.loadOfflinePushStatusesAndPartitionStatuses();
      } catch (Throwable t) {
        loadFailure.set(t);
      }
      interruptedAfterLoad.set(Thread.currentThread().isInterrupted());
    });
    loader.start();
    Assert.assertTrue(readsStarted.await(30, TimeUnit.SECONDS));
    loader.interrupt();
    loader.join(TimeUnit.SECONDS.toMillis(30));

    Assert.assertFalse(loader.isAlive());
    Assert.assertTrue(loadFailure.get() instanceof VeniceException, "Unexpected failure: " + loadFailure.get());
    Assert.assertTrue(loadFailure.get().getMessage().contains("Interrupted"), loadFailure.get().getMessage());
    Assert.assertTrue(interruptedAfterLoad.get());
    verify(mockPartitionStatusAccessor, never()).getChildren(anyString(), isNull(), anyInt());
  }

  private static OfflinePushStatus createPushStatus(String topic, int partitionCount, ExecutionStatus status) {
    OfflinePushStatus pushStatus =
        new OfflinePushStatus(topic, partitionCount, 3, OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
    pushStatus.setCurrentStatus(status);
    return pushStatus;
  }

  private static void stubPartitionStatuses(
      ZkBaseDataAccessor<PartitionStatus> mockPartitionStatusAccessor,
      String pushPath,
      String topic,
      int partitionCount) {
    List<String> partitionNames = new ArrayList<>();
    List<PartitionStatus> partitionStatuses = new ArrayList<>();
    for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
      PartitionStatus partitionStatus = new PartitionStatus(partitionId);
      partitionStatus.updateReplicaStatus(topic + "_instance_" + partitionId, ExecutionStatus.COMPLETED);
      partitionNames.add(String.valueOf(partitionId));
      partitionStatuses.add(partitionStatus);
    }
    doReturn(partitionNames).when(mockPartitionStatusAccessor).getChildNames(pushPath, AccessOption.PERSISTENT);
    doReturn(partitionStatuses).when(mockPartitionStatusAccessor).getChildren(pushPath, null, AccessOption.PERSISTENT);
  }

  private static LogContext getLogContext() {
    return LogContext.newBuilder()
        .setComponentName(VeniceComponent.CONTROLLER.name())
        .setRegionName("test-region")
        .build();
  }
}
