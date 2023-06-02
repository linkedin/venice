package com.linkedin.venice;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponse;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.controllerapi.MultiStoreStatusResponse;
import com.linkedin.venice.controllerapi.StoreHealthAuditResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.datarecovery.Command;
import com.linkedin.venice.datarecovery.DataRecoveryClient;
import com.linkedin.venice.datarecovery.DataRecoveryEstimator;
import com.linkedin.venice.datarecovery.DataRecoveryExecutor;
import com.linkedin.venice.datarecovery.DataRecoveryMonitor;
import com.linkedin.venice.datarecovery.DataRecoveryTask;
import com.linkedin.venice.datarecovery.EstimateDataRecoveryTimeCommand;
import com.linkedin.venice.datarecovery.MonitorCommand;
import com.linkedin.venice.datarecovery.StoreRepushCommand;
import com.linkedin.venice.meta.RegionPushDetails;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.UncompletedPartition;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestDataRecoveryClient {
  private DataRecoveryExecutor executor;
  private DataRecoveryEstimator estimator;
  private DataRecoveryMonitor monitor;

  @Test
  public void testEstimator() {
    estimateRecovery();
    verifyEstimationResults();
  }

  @Test
  public void testExecutor() {
    for (boolean isSuccess: new boolean[] { true, false }) {
      executeRecovery(isSuccess);
      verifyExecuteRecoveryResults(isSuccess);
    }
  }

  private void verifyEstimationResults() {
    Long expectedRecoveryTime = 7200L;
    Long result = 0L;
    for (DataRecoveryTask t: estimator.getTasks()) {
      result += ((EstimateDataRecoveryTimeCommand.Result) t.getTaskResult().getCmdResult())
          .getEstimatedRecoveryTimeInSeconds();
    }
    Assert.assertEquals(result, expectedRecoveryTime);
  }

  private void verifyExecuteRecoveryResults(boolean isSuccess) {
    int numOfStores = 3;
    Assert.assertEquals(executor.getTasks().size(), numOfStores);
    if (isSuccess) {
      // Verify all stores are executed successfully.
      for (int i = 0; i < numOfStores; i++) {
        Assert.assertFalse(executor.getTasks().get(i).getTaskResult().isError());
      }
    } else {
      // Verify all stores are executed unsuccessfully.
      for (int i = 0; i < numOfStores; i++) {
        // For tasks that require sentinel run, if the first task is errored, then the remaining task wouldn't be
        // executed.
        if (i == 0) {
          Assert.assertTrue(executor.getTasks().get(i).getTaskResult().isError());
        } else {
          Assert.assertNull(executor.getTasks().get(i).getTaskResult());
        }
      }
    }
  }

  private void estimateRecovery() {
    estimator = spy(DataRecoveryEstimator.class);
    ControllerClient controllerClient = mock(ControllerClient.class);

    Set<String> storeNames = new HashSet<>(Arrays.asList("store1", "store2"));
    EstimateDataRecoveryTimeCommand.Params cmdParams = new EstimateDataRecoveryTimeCommand.Params();
    cmdParams.setTargetRegion("region1");
    cmdParams.setParentUrl("https://localhost:7036");
    cmdParams.setPCtrlCliWithoutCluster(controllerClient);
    EstimateDataRecoveryTimeCommand mockCmd = spy(EstimateDataRecoveryTimeCommand.class);
    List<DataRecoveryTask> tasks = buildTasks(storeNames, mockCmd, cmdParams);
    doReturn(cmdParams).when(mockCmd).getParams();
    D2ServiceDiscoveryResponse r = new D2ServiceDiscoveryResponse();
    r.setCluster("test");
    doReturn(r).when(controllerClient).discoverCluster(anyString());
    doReturn(controllerClient).when(mockCmd).buildControllerClient(any(), any(), any());

    doReturn(tasks).when(estimator).buildTasks(any(), any());
    DataRecoveryClient dataRecoveryClient = mock(DataRecoveryClient.class);
    doReturn(estimator).when(dataRecoveryClient).getEstimator();
    doCallRealMethod().when(dataRecoveryClient).estimateRecoveryTime(any(), any());

    StoreHealthAuditResponse mockResponse = new StoreHealthAuditResponse();

    RegionPushDetails det = new RegionPushDetails();
    det.setPushStartTimestamp("2023-03-09T00:20:15.063472");
    det.setPushEndTimestamp("2023-03-09T01:20:15.063472");
    RegionPushDetails det2 = new RegionPushDetails();
    det2.setPushStartTimestamp("2023-03-09T00:20:15.063472");
    det2.setPushEndTimestamp("2023-03-09T01:20:15.063472");

    mockResponse.setRegionPushDetails(new HashMap<String, RegionPushDetails>() {
      {
        put("region1", det);
        put("region2", det2);
      }
    });

    doReturn(mockResponse).when(controllerClient).listStorePushInfo(anyString(), anyBoolean());
    doReturn("test").when(controllerClient).getClusterName();

    dataRecoveryClient.estimateRecoveryTime(new DataRecoveryClient.DataRecoveryParams("store1,store2"), cmdParams);
  }

  private void executeRecovery(boolean isSuccess) {
    StoreRepushCommand.Params cmdParams = new StoreRepushCommand.Params();
    cmdParams.setCommand("cmd");
    cmdParams.setExtraCommandArgs("args");

    // Partial mock of Module class to take password from console input.
    executor = spy(DataRecoveryExecutor.class);

    // Mock command to mimic a successful repush result.
    List<String> mockCmd = new ArrayList<>();
    mockCmd.add("sh");
    mockCmd.add("-c");

    if (isSuccess) {
      mockCmd.add("echo \"success: https://example.com/executor?execid=21585379\"");
    } else {
      mockCmd.add("echo \"failure: Incorrect Login. Username/Password+VIP not found.\"");
    }
    StoreRepushCommand mockStoreRepushCmd = spy(StoreRepushCommand.class);
    mockStoreRepushCmd.setParams(cmdParams);
    doReturn(mockCmd).when(mockStoreRepushCmd).getShellCmd();

    // Inject the mocked command into the running system.
    Set<String> storeName = new HashSet<>(Arrays.asList("store1", "store2", "store3"));
    List<DataRecoveryTask> tasks = buildTasks(storeName, mockStoreRepushCmd, cmdParams);
    doReturn(tasks).when(executor).buildTasks(any(), any());

    // Partial mock of Client class to confirm to-be-repushed stores from standard input.
    DataRecoveryClient dataRecoveryClient = mock(DataRecoveryClient.class);
    doReturn(executor).when(dataRecoveryClient).getExecutor();
    doCallRealMethod().when(dataRecoveryClient).execute(any(), any());
    doReturn(true).when(dataRecoveryClient).confirmStores(any());
    // client executes three store recovery.
    DataRecoveryClient.DataRecoveryParams drParams = new DataRecoveryClient.DataRecoveryParams("store1,store2,store3");
    drParams.setNonInteractive(true);
    dataRecoveryClient.execute(drParams, cmdParams);
  }

  private List<DataRecoveryTask> buildTasks(Set<String> storeNames, Command cmd, Command.Params params) {
    List<DataRecoveryTask> tasks = new ArrayList<>();
    for (String name: storeNames) {
      DataRecoveryTask.TaskParams taskParams = new DataRecoveryTask.TaskParams(name, params);
      tasks.add(new DataRecoveryTask(cmd, taskParams));
    }
    return tasks;
  }

  @Test
  public void testMonitor() {
    for (ExecutionStatus status: new ExecutionStatus[] { ExecutionStatus.STARTED, ExecutionStatus.COMPLETED,
        ExecutionStatus.ERROR }) {
      for (boolean isCurrentVersionNewer: new boolean[] { true, false }) {
        monitorRecovery(status, isCurrentVersionNewer);
        verifyMonitorRecoveryResults(status, isCurrentVersionNewer);
      }
    }
  }

  private void monitorRecovery(ExecutionStatus status, boolean isCurrentVersionNewer) {
    int storePartitionCount = 100;
    int version = 10;
    String region = "ei";

    monitor = spy(DataRecoveryMonitor.class);

    D2ServiceDiscoveryResponse discoveryResponse = new D2ServiceDiscoveryResponse();
    discoveryResponse.setCluster("test");

    MultiStoreStatusResponse statusResponse = new MultiStoreStatusResponse();
    statusResponse.setStoreStatusMap(Collections.singletonMap(region, String.valueOf(version)));

    JobStatusQueryResponse queryResponse = buildJobStatusQueryResponse(status, version);
    StoreResponse storeResponse = buildStoreResponse(storePartitionCount);

    LocalDateTime now = LocalDateTime.now();
    StoreHealthAuditResponse storeHealthResp =
        buildStoreHealthAuditResponse(region, isCurrentVersionNewer ? now.plusMinutes(1) : now.minusMinutes(1));

    // Mock ControllerClient functions.
    ControllerClient mockedCli = mock(ControllerClient.class);
    doReturn(storeResponse).when(mockedCli).getStore(anyString());
    doReturn(discoveryResponse).when(mockedCli).discoverCluster(anyString());
    doReturn(statusResponse).when(mockedCli).getFutureVersions(anyString(), anyString());
    doReturn(queryResponse).when(mockedCli).queryDetailedJobStatus(any(), anyString());
    doReturn(storeHealthResp).when(mockedCli).listStorePushInfo(anyString(), anyBoolean());

    DataRecoveryClient dataRecoveryClient = mock(DataRecoveryClient.class);
    doReturn(monitor).when(dataRecoveryClient).getMonitor();
    doCallRealMethod().when(dataRecoveryClient).monitor(any(), any());

    MonitorCommand.Params monitorParams = new MonitorCommand.Params();
    monitorParams.setTargetRegion(region);
    monitorParams.setDateTime(now);
    monitorParams.setPCtrlCliWithoutCluster(mockedCli);

    MonitorCommand mockMonitorCmd = spy(MonitorCommand.class);
    mockMonitorCmd.setParams(monitorParams);
    mockMonitorCmd.setOngoingOfflinePushDetected(false);
    doReturn(mockedCli).when(mockMonitorCmd).buildControllerClient(any(), any(), any());

    // Inject the mocked command into the running system.
    Set<String> storeName = new HashSet<>(Arrays.asList("store1", "store2", "store3"));
    List<DataRecoveryTask> tasks = buildTasks(storeName, mockMonitorCmd, monitorParams);
    doReturn(tasks).when(monitor).buildTasks(any(), any());

    // client monitors three store recovery progress.
    DataRecoveryClient.DataRecoveryParams drParams = new DataRecoveryClient.DataRecoveryParams("store1,store2,store3");
    dataRecoveryClient.monitor(drParams, monitorParams);
  }

  private StoreResponse buildStoreResponse(int storePartitionCount) {
    StoreResponse storeResponse = new StoreResponse();
    storeResponse.setStore(new StoreInfo());
    storeResponse.getStore().setPartitionCount(storePartitionCount);
    return storeResponse;
  }

  private JobStatusQueryResponse buildJobStatusQueryResponse(ExecutionStatus status, int version) {
    JobStatusQueryResponse jobResponse = new JobStatusQueryResponse();
    jobResponse.setStatus(status.toString());
    jobResponse.setVersion(version);

    // If overall status is STARTED, let's assume there are 10 uncompleted partitions.
    if (status == ExecutionStatus.STARTED) {
      int numOfUncompleted = 10;
      List<UncompletedPartition> partitions = new ArrayList<>();
      for (int id = 0; id < numOfUncompleted; id++) {
        UncompletedPartition partition = new UncompletedPartition();
        partition.setPartitionId(id);
        partitions.add(partition);
      }
      jobResponse.setUncompletedPartitions(partitions);
    }

    if (status == ExecutionStatus.ERROR) {
      jobResponse.setStatusDetails(
          "too many ERROR replicas in partition: x for offlinePushStrategy: WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION");
    }
    return jobResponse;
  }

  private StoreHealthAuditResponse buildStoreHealthAuditResponse(String region, LocalDateTime dateTime) {
    StoreHealthAuditResponse resp = new StoreHealthAuditResponse();
    RegionPushDetails details = new RegionPushDetails();
    details.setPushStartTimestamp(dateTime.toString());
    // build a map with one entry <region, details>.
    resp.setRegionPushDetails(Collections.singletonMap(region, details));
    return resp;
  }

  private void verifyMonitorRecoveryResults(ExecutionStatus status, boolean isCurrentVersionNewer) {
    int numOfStores = 3;
    Assert.assertEquals(monitor.getTasks().size(), numOfStores);

    // If isCurrentVersionNewer is set to true, then we can claim that current version is good enough and done.
    if (isCurrentVersionNewer) {
      for (int i = 0; i < numOfStores; i++) {
        Assert.assertTrue(monitor.getTasks().get(i).getTaskResult().isCoreWorkDone());
        Assert.assertNotNull(monitor.getTasks().get(i).getTaskResult().getMessage());
      }
      return;
    }

    if (status == ExecutionStatus.STARTED) {
      // Verify all stores in uncompleted state.
      for (int i = 0; i < numOfStores; i++) {
        Assert.assertFalse(monitor.getTasks().get(i).getTaskResult().isCoreWorkDone());
        Assert.assertNotNull(monitor.getTasks().get(i).getTaskResult().getMessage());
      }
      return;
    }

    if (status == ExecutionStatus.COMPLETED) {
      // Verify all stores are finished.
      for (int i = 0; i < numOfStores; i++) {
        Assert.assertTrue(monitor.getTasks().get(i).getTaskResult().isCoreWorkDone());
        Assert.assertNotNull(monitor.getTasks().get(i).getTaskResult().getMessage());
      }
      return;
    }

    if (status == ExecutionStatus.ERROR) {
      // Verify all stores are in error state.
      for (int i = 0; i < numOfStores; i++) {
        Assert.assertTrue(monitor.getTasks().get(i).getTaskResult().isError());
        Assert.assertNull(monitor.getTasks().get(i).getTaskResult().getMessage());
      }
    }
  }
}
