package com.linkedin.venice.pushmonitor;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreCleaner;
import com.linkedin.venice.replication.TopicReplicator;
import com.linkedin.venice.utils.Pair;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;

public class HelixEVBasedPushMonitorTest extends AbstractPushMonitorTest {
  @Override
  protected AbstractPushMonitor getPushMonitor(StoreCleaner storeCleaner) {
    return new HelixEVBasedPushMonitor(getClusterName(), getMockRoutingDataRepo(), getMockAccessor(), storeCleaner,
        getMockStoreRepo(), getMockPushHealthStats(), false, Optional.of(mock(TopicReplicator.class)),
        getMockMetadataStoreWriter(), getClusterLockManager(), getAggregateRealTimeSourceKafkaUrl());
  }

  @Override
  protected AbstractPushMonitor getPushMonitor(boolean skipBufferReplayForHybrid, TopicReplicator mockReplicator) {
    return new HelixEVBasedPushMonitor(getClusterName(), getMockRoutingDataRepo(), getMockAccessor(), getMockStoreCleaner(),
        getMockStoreRepo(), getMockPushHealthStats(), skipBufferReplayForHybrid, Optional.of(mockReplicator),
        getMockMetadataStoreWriter(), getClusterLockManager(), getAggregateRealTimeSourceKafkaUrl());
  }

  @Test
  public void testLoadRunningPushWhichIsNotUpdateToDate() {
    String topic = getTopic();
    Store store = prepareMockStore(topic);
    List<OfflinePushStatus> statusList = new ArrayList<>();
    OfflinePushStatus pushStatus = new OfflinePushStatus(topic, getNumberOfPartition(), getReplicationFactor(),
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
    statusList.add(pushStatus);
    doReturn(statusList).when(getMockAccessor()).loadOfflinePushStatusesAndPartitionStatuses();
    PartitionAssignment partitionAssignment = new PartitionAssignment(topic, getNumberOfPartition());
    doReturn(true).when(getMockRoutingDataRepo()).containsKafkaTopic(eq(topic));
    doReturn(partitionAssignment).when(getMockRoutingDataRepo()).getPartitionAssignments(topic);
    PushStatusDecider decider = mock(PushStatusDecider.class);
    Pair<ExecutionStatus, Optional<String>> statusAndDetails = new Pair<>(ExecutionStatus.COMPLETED, Optional.empty());
    doReturn(statusAndDetails).when(decider).checkPushStatusAndDetails(pushStatus, partitionAssignment);
    PushStatusDecider.updateDecider(OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION, decider);
    when(getMockAccessor().getOfflinePushStatusAndItsPartitionStatuses(Mockito.anyString())).thenAnswer(invocation ->
    {
      String kafkaTopic = invocation.getArgument(0);
      for(OfflinePushStatus status : statusList) {
        if(status.getKafkaTopic().equals(kafkaTopic)) {
          return status;
        }
      }
      return null;
    });
    getMonitor().loadAllPushes();
    verify(getMockStoreRepo(), atLeastOnce()).updateStore(store);
    verify(getMockStoreCleaner(), atLeastOnce())
        .retireOldStoreVersions(anyString(), anyString(), eq(false));
    Assert.assertEquals(getMonitor().getOfflinePushOrThrow(topic).getCurrentStatus(), ExecutionStatus.COMPLETED);
    // After offline push completed, bump up the current version of this store.
    Assert.assertEquals(store.getCurrentVersion(), 1);

    //set the push status decider back
    PushStatusDecider.updateDecider(OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION, new WaitNMinusOnePushStatusDecider());
    Mockito.reset(getMockAccessor());
  }

  @Test
  public void testLoadRunningPushWhichIsNotUpdateToDateAndDeletionError() {
    String topic = getTopic();
    Store store = prepareMockStore(topic);
    List<OfflinePushStatus> statusList = new ArrayList<>();
    OfflinePushStatus pushStatus = new OfflinePushStatus(topic, getNumberOfPartition(), getReplicationFactor(),
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
    statusList.add(pushStatus);
    doReturn(statusList).when(getMockAccessor()).loadOfflinePushStatusesAndPartitionStatuses();
    PartitionAssignment partitionAssignment = new PartitionAssignment(topic, getNumberOfPartition());
    doReturn(true).when(getMockRoutingDataRepo()).containsKafkaTopic(eq(topic));
    doReturn(partitionAssignment).when(getMockRoutingDataRepo()).getPartitionAssignments(topic);
    PushStatusDecider decider = mock(PushStatusDecider.class);
    Pair<ExecutionStatus, Optional<String>> statusAndDetails = new Pair<>(ExecutionStatus.ERROR, Optional.empty());
    doReturn(statusAndDetails).when(decider).checkPushStatusAndDetails(pushStatus, partitionAssignment);
    PushStatusDecider.updateDecider(OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION, decider);
    doThrow(new VeniceException("Could not delete.")).when(getMockStoreCleaner())
        .deleteOneStoreVersion(anyString(), anyString(), anyInt());

    when(getMockAccessor().getOfflinePushStatusAndItsPartitionStatuses(Mockito.anyString())).thenAnswer(invocation ->
    {
      String kafkaTopic = invocation.getArgument(0);
      for(OfflinePushStatus status : statusList) {
        if(status.getKafkaTopic().equals(kafkaTopic)) {
          return status;
        }
      }
      return null;
    });

    getMonitor().loadAllPushes();
    verify(getMockStoreRepo(), atLeastOnce()).updateStore(store);
    verify(getMockStoreCleaner(), atLeastOnce())
        .deleteOneStoreVersion(anyString(), anyString(), anyInt());
    Assert.assertEquals(getMonitor().getOfflinePushOrThrow(topic).getCurrentStatus(), ExecutionStatus.ERROR);

    //set the push status decider back
    PushStatusDecider.updateDecider(OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION, new WaitNMinusOnePushStatusDecider());
    Mockito.reset(getMockAccessor());
  }

  @DataProvider(name = "pushStatues")
  public static Object[][] pushStatues() {
    return new Object[][]{{ExecutionStatus.COMPLETED}, {ExecutionStatus.STARTED}, {ExecutionStatus.ERROR}};
  }

  @Test(dataProvider = "pushStatues")
  public void testOnRoutingDataChanged(ExecutionStatus expectedStatus) {
    String topic = getTopic();
    prepareMockStore(topic);

    getMonitor().startMonitorOfflinePush(topic, getNumberOfPartition(), getReplicationFactor(),
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
    PartitionAssignment partitionAssignment = new PartitionAssignment(topic, getNumberOfPartition());
    OfflinePushStatus pushStatus = getMonitor().getOfflinePushOrThrow(topic);
    PushStatusDecider decider = mock(PushStatusDecider.class);
    Pair<ExecutionStatus, Optional<String>> statusAndDetails = new Pair<>(expectedStatus, Optional.empty());
    doReturn(statusAndDetails).when(decider).checkPushStatusAndDetails(pushStatus, partitionAssignment);
    PushStatusDecider.updateDecider(OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION, decider);
    ((HelixEVBasedPushMonitor) getMonitor()).onExternalViewChange(partitionAssignment);
    Assert.assertEquals(getMonitor().getOfflinePushOrThrow(topic).getCurrentStatus(), expectedStatus);
    if (expectedStatus.equals(ExecutionStatus.COMPLETED)) {
      verify(getMockPushHealthStats(), times(1)).recordSuccessfulPush(eq(getStoreName()), anyLong());
    } else if (expectedStatus.equals(ExecutionStatus.ERROR)) {
      verify(getMockPushHealthStats(), times(1)).recordFailedPush(eq(getStoreName()), anyLong());
    }

    //set the push status decider back
    PushStatusDecider.updateDecider(OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION, new WaitNMinusOnePushStatusDecider());
  }
}
