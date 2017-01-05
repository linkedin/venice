package com.linkedin.venice.pushmonitor;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.job.ExecutionStatus;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.RoutingDataRepository;
import java.util.ArrayList;
import java.util.List;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class OfflinePushMonitorTest {
  private RoutingDataRepository mockRoutingDataRepo;
  private OfflinePushAccessor mockAccessor;
  private OfflinePushMonitor monitor;
  private int numberOfPartition = 1;
  private int replicationFator = 3;

  @BeforeMethod
  public void setup() {
    mockRoutingDataRepo = Mockito.mock(RoutingDataRepository.class);
    mockAccessor = Mockito.mock(OfflinePushAccessor.class);
    monitor = new OfflinePushMonitor("OfflinePushMonitorTest", mockRoutingDataRepo, mockAccessor);
  }

  @Test
  public void testStartMonitorOfflinePush() {
    String topic = "testStartMonitorOfflinePush";
    monitor.startMonitorOfflinePush(topic, numberOfPartition, replicationFator,
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
    OfflinePushStatus pushStatus = monitor.getOfflinePush(topic);
    Assert.assertEquals(pushStatus.getCurrentStatus(), ExecutionStatus.STARTED);
    Assert.assertEquals(pushStatus.getKafkaTopic(), topic);
    Assert.assertEquals(pushStatus.getNumberOfPartition(), numberOfPartition);
    Assert.assertEquals(pushStatus.getReplicationFactor(), replicationFator);
    Mockito.verify(mockAccessor, Mockito.atLeastOnce()).createOfflinePushStatusAndItsPartitionStatuses(pushStatus);
    Mockito.verify(mockAccessor, Mockito.atLeastOnce()).subscribePartitionStatusChange(pushStatus, monitor);
    Mockito.verify(mockRoutingDataRepo, Mockito.atLeastOnce()).subscribeRoutingDataChange(topic, monitor);
    try {
      monitor.startMonitorOfflinePush(topic, numberOfPartition, replicationFator,
          OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
      Assert.fail("Duplicated monitoring is not allowed. ");
    } catch (VeniceException e) {
    }
  }

  @Test
  public void testStopMonitorOfflinePush() {
    String topic = "testStopMonitorOfflinePush";
    monitor.startMonitorOfflinePush(topic, numberOfPartition, replicationFator,
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
    OfflinePushStatus pushStatus = monitor.getOfflinePush(topic);
    monitor.stopMonitorOfflinePush(topic);
    Mockito.verify(mockAccessor, Mockito.atLeastOnce()).deleteOfflinePushStatusAndItsPartitionStatuses(pushStatus);
    Mockito.verify(mockAccessor, Mockito.atLeastOnce()).unsubscribePartitionsStatusChange(pushStatus, monitor);
    Mockito.verify(mockRoutingDataRepo, Mockito.atLeastOnce()).unSubscribeRoutingDataChange(topic, monitor);

    try {
      monitor.getOfflinePush(topic);
      Assert.fail("Push status should be deleted by stopMonitorOfflinePush method");
    } catch (VeniceException e) {
    }
  }

  @Test
  public void testLoadAllPushes() {
    int statusCount = 3;
    List<OfflinePushStatus> statusList = new ArrayList<>(statusCount);
    for (int i = 0; i < statusCount; i++) {
      OfflinePushStatus pushStatus = new OfflinePushStatus("testLoadAllPushes" + i, numberOfPartition, replicationFator,
          OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
      pushStatus.setCurrentStatus(ExecutionStatus.COMPLETED);
      statusList.add(pushStatus);
    }
    Mockito.doReturn(statusList).when(mockAccessor).loadOfflinePushStatusesAndPartitionStatuses();
    monitor.loadAllPushes();
    for (int i = 0; i < statusCount; i++) {
      Assert.assertEquals(monitor.getOfflinePush("testLoadAllPushes" + i).getCurrentStatus(),
          ExecutionStatus.COMPLETED);
    }
  }

  @Test
  public void testLoadRunningPushWhichIsNotUpdateToDate() {
    String topic = "testLoadRunningPushWhichIsNotUpdateToDate";
    List<OfflinePushStatus> statusList = new ArrayList<>();
    OfflinePushStatus pushStatus = new OfflinePushStatus(topic, numberOfPartition, replicationFator,
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
    statusList.add(pushStatus);
    Mockito.doReturn(statusList).when(mockAccessor).loadOfflinePushStatusesAndPartitionStatuses();
    PartitionAssignment partitionAssignment = new PartitionAssignment(topic, numberOfPartition);
    Mockito.doReturn(partitionAssignment).when(mockRoutingDataRepo).getPartitionAssignments(topic);
    PushStatusDecider decider = Mockito.mock(PushStatusDecider.class);
    Mockito.doReturn(ExecutionStatus.COMPLETED).when(decider).checkPushStatus(pushStatus, partitionAssignment);
    PushStatusDecider.updateDecider(OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION, decider);

    monitor.loadAllPushes();
    Assert.assertEquals(monitor.getOfflinePush(topic).getCurrentStatus(), ExecutionStatus.COMPLETED);
  }

  @DataProvider(name = "pushStatues")
  public static Object[][] pushStatues() {
    return new Object[][]{{ExecutionStatus.COMPLETED}, {ExecutionStatus.STARTED}, {ExecutionStatus.ERROR}};
  }

  @Test(dataProvider = "pushStatues")
  public void testOnRoutingDataChanged(ExecutionStatus expectedStatus) {
    String topic = "testOnRoutingDataChanged";

    monitor.startMonitorOfflinePush(topic, numberOfPartition, numberOfPartition,
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
    PartitionAssignment partitionAssignment = new PartitionAssignment(topic, numberOfPartition);
    OfflinePushStatus pushStatus = monitor.getOfflinePush(topic);
    PushStatusDecider decider = Mockito.mock(PushStatusDecider.class);
    Mockito.doReturn(expectedStatus).when(decider).checkPushStatus(pushStatus, partitionAssignment);
    PushStatusDecider.updateDecider(OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION, decider);
    monitor.onRoutingDataChanged(partitionAssignment);
    Assert.assertEquals(monitor.getOfflinePush(topic).getCurrentStatus(), expectedStatus);
  }
}
