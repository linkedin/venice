package com.linkedin.venice;

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
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
import com.linkedin.venice.datarecovery.meta.RepushViabilityInfo;
import com.linkedin.venice.meta.HybridStoreConfigImpl;
import com.linkedin.venice.meta.RegionPushDetails;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.UncompletedPartition;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
    }
  }

  private void verifyEstimationResults() {
    Long expectedRecoveryTime = 7200L;
    Long result = 0L;
    Set<String> storeNames = new HashSet<>();
    for (DataRecoveryTask t: estimator.getTasks()) {
      result += ((EstimateDataRecoveryTimeCommand.Result) t.getTaskResult().getCmdResult())
          .getEstimatedRecoveryTimeInSeconds();
      Assert.assertFalse(storeNames.contains(t.getTaskParams().getStore()));
      storeNames.add(t.getTaskParams().getStore());
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
      Set<String> storeNames = new HashSet<>();
      // Double-check the stores recovered. We do not want to see duplicates in the final report.
      for (int i = 0; i < numOfStores; i++) {
        String storeName = executor.getTasks().get(i).getTaskParams().getCmdParams().getStore();
        Assert.assertFalse(storeNames.contains(storeName));
        storeNames.add(storeName);
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
    EstimateDataRecoveryTimeCommand.Params.Builder builder =
        new EstimateDataRecoveryTimeCommand.Params.Builder().setTargetRegion("region1")
            .setParentUrl("https://localhost:7036")
            .setPCtrlCliWithoutCluster(controllerClient)
            .setSSLFactory(Optional.empty());
    EstimateDataRecoveryTimeCommand.Params cmdParams = builder.build();
    EstimateDataRecoveryTimeCommand mockCmd = spy(EstimateDataRecoveryTimeCommand.class);
    List<DataRecoveryTask> tasks = buildEstimateDataRecoveryTasks(storeNames, mockCmd, cmdParams);
    doReturn(cmdParams).when(mockCmd).getParams();
    D2ServiceDiscoveryResponse r = new D2ServiceDiscoveryResponse();
    r.setCluster("test");
    doReturn(r).when(controllerClient).discoverCluster(anyString());
    doReturn(controllerClient).when(mockCmd).buildControllerClient(anyString(), anyString(), any());

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
    doReturn(cmdParams).when(mockCmd).getParams();

    dataRecoveryClient.estimateRecoveryTime(new DataRecoveryClient.DataRecoveryParams(storeNames), cmdParams);
  }

  private void executeRecovery(boolean isSuccess) {
    ControllerClient controllerClient = mock(ControllerClient.class);
    StoreRepushCommand.Params.Builder builder = new StoreRepushCommand.Params.Builder().setCommand("cmd")
        .setExtraCommandArgs("args")
        .setPCtrlCliWithoutCluster(controllerClient)
        .setUrl("https://localhost:7036")
        .setTimestamp(LocalDateTime.parse("2999-12-31T00:00:00", DateTimeFormatter.ISO_LOCAL_DATE_TIME))
        .setSSLFactory(Optional.empty())
        .setDestFabric("ei-ltx1")
        .setSourceFabric("ei4");

    D2ServiceDiscoveryResponse r = new D2ServiceDiscoveryResponse();
    r.setCluster("test");
    doReturn(r).when(controllerClient).discoverCluster(anyString());

    StoreRepushCommand.Params cmdParams = builder.build();

    ControllerResponse mockRecoveryResponse = spy(ControllerResponse.class);
    doReturn(false).when(mockRecoveryResponse).isError();
    doReturn(mockRecoveryResponse).when(controllerClient)
        .prepareDataRecovery(anyString(), anyString(), anyString(), anyInt(), any());
    doReturn(mockRecoveryResponse).when(controllerClient)
        .dataRecovery(anyString(), anyString(), anyString(), anyInt(), anyBoolean(), anyBoolean(), any());

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
    doCallRealMethod().when(mockStoreRepushCmd).toString();
    doReturn(true).when(mockStoreRepushCmd).isShellCmdExecuted();
    doReturn(new StoreRepushCommand.Result()).when(mockStoreRepushCmd).getResult();

    // Inject the mocked command into the running system.
    Set<String> storeName = new HashSet<>(Arrays.asList("store1", "store2", "store3"));
    List<DataRecoveryTask> tasks =
        buildExecuteDataRecoveryTasks(storeName, mockStoreRepushCmd, cmdParams, isSuccess, controllerClient);
    doReturn(tasks).when(executor).buildTasks(any(), any());

    // Store filtering mocks
    StoreHealthAuditResponse storeHealthInfoMock = mock(StoreHealthAuditResponse.class);
    Map<String, RegionPushDetails> regionPushDetailsMapMock = new HashMap<>();
    RegionPushDetails regionPushDetailsMock = mock(RegionPushDetails.class);
    doReturn("1970-12-31T23:59:59.171961").when(regionPushDetailsMock).getPushStartTimestamp();
    regionPushDetailsMapMock.put("ei-ltx1", regionPushDetailsMock);
    doReturn(regionPushDetailsMapMock).when(storeHealthInfoMock).getRegionPushDetails();
    doReturn(storeHealthInfoMock).when(controllerClient).listStorePushInfo(anyString(), anyBoolean());

    MultiStoreStatusResponse storeStatusResponse = mock(MultiStoreStatusResponse.class);
    Map<String, String> storeStatusMap = new HashMap<>();
    storeStatusMap.put("ei-ltx1", "0");
    doReturn(storeStatusMap).when(storeStatusResponse).getStoreStatusMap();
    doReturn(storeStatusResponse).when(controllerClient).getFutureVersions(anyString(), anyString());

    StoreResponse storeResponse = mock(StoreResponse.class);
    StoreInfo storeInfo = new StoreInfo();
    storeInfo.setHybridStoreConfig(new HybridStoreConfigImpl(0L, 0L, 0L, null, null));
    storeInfo.setCurrentVersion(1);
    doReturn(storeInfo).when(storeResponse).getStore();
    doReturn(storeResponse).when(controllerClient).getStore(anyString());

    // Partial mock of Client class to confirm to-be-repushed stores from standard input.
    DataRecoveryClient dataRecoveryClient = mock(DataRecoveryClient.class);
    doReturn(executor).when(dataRecoveryClient).getExecutor();
    doCallRealMethod().when(dataRecoveryClient).execute(any(), any());
    doReturn(true).when(dataRecoveryClient).confirmStores(any());
    // client executes three store recovery.
    DataRecoveryClient.DataRecoveryParams drParams = new DataRecoveryClient.DataRecoveryParams(storeName);
    drParams.setNonInteractive(true);

    dataRecoveryClient.execute(drParams, cmdParams);
    verifyExecuteRecoveryResults(isSuccess);
    dataRecoveryClient.getExecutor().getTasks().clear();

    // test batch store
    storeInfo.setHybridStoreConfig(null);
    dataRecoveryClient.execute(drParams, cmdParams);
    for (DataRecoveryTask t: dataRecoveryClient.getExecutor().getTasks()) {
      Assert.assertFalse(t.getTaskResult().getCmdResult().isError());
    }

    dataRecoveryClient.getExecutor().getTasks().clear();

    // testing repush with invalid timestamps

    storeStatusMap.put("ei-ltx1", "7");
    builder.setTimestamp(LocalDateTime.parse("1999-12-31T00:00:00", DateTimeFormatter.ISO_LOCAL_DATE_TIME));
    cmdParams = builder.build();
    doReturn("2999-12-31T23:59:59.171961").when(regionPushDetailsMock).getPushStartTimestamp();
    Assert.assertEquals(
        storeHealthInfoMock.getRegionPushDetails().get("ei-ltx1").getPushStartTimestamp(),
        "2999-12-31T23:59:59.171961");
    dataRecoveryClient.execute(drParams, cmdParams);

    // verify
    for (DataRecoveryTask t: dataRecoveryClient.getExecutor().getTasks()) {
      Assert.assertTrue(t.getCommand().getResult().isError());
    }

    drParams = new DataRecoveryClient.DataRecoveryParams(null);
    dataRecoveryClient.execute(drParams, cmdParams);
  }

  private List<DataRecoveryTask> buildExecuteDataRecoveryTasks(
      Set<String> storeNames,
      Command cmd,
      StoreRepushCommand.Params params,
      boolean isSuccess,
      ControllerClient controllerClient) {
    List<DataRecoveryTask> tasks = new ArrayList<>();
    StoreRepushCommand.Params.Builder builder = new StoreRepushCommand.Params.Builder(params);
    for (String name: storeNames) {
      builder.setPCtrlCliWithoutCluster(controllerClient);
      StoreRepushCommand.Params p = builder.build();
      p.setStore(name);
      DataRecoveryTask.TaskParams taskParams = new DataRecoveryTask.TaskParams(name, p);
      StoreRepushCommand newCmd = spy(StoreRepushCommand.class);
      doReturn(controllerClient).when(newCmd).buildControllerClient(anyString(), anyString(), any());
      doReturn(p).when(newCmd).getParams();
      List<String> mockCmd = new ArrayList<>();
      mockCmd.add("sh");
      mockCmd.add("-c");

      if (isSuccess) {
        mockCmd.add("echo \"success: https://example.com/executor?execid=21585379\"");
      } else {
        mockCmd.add("echo \"failure: Incorrect Login. Username/Password+VIP not found.\"");
      }
      doReturn(mockCmd).when(newCmd).getShellCmd();

      doCallRealMethod().when(newCmd).toString();
      doReturn(true).when(newCmd).isShellCmdExecuted();
      tasks.add(new DataRecoveryTask(newCmd, taskParams));
    }
    return tasks;
  }

  private List<DataRecoveryTask> buildEstimateDataRecoveryTasks(
      Set<String> storeNames,
      Command cmd,
      EstimateDataRecoveryTimeCommand.Params params) {
    List<DataRecoveryTask> tasks = new ArrayList<>();
    EstimateDataRecoveryTimeCommand.Params.Builder builder = new EstimateDataRecoveryTimeCommand.Params.Builder(params);
    for (String name: storeNames) {
      EstimateDataRecoveryTimeCommand.Params p = builder.build();
      p.setStore(name);
      DataRecoveryTask.TaskParams taskParams = new DataRecoveryTask.TaskParams(name, p);
      EstimateDataRecoveryTimeCommand newCmd = spy(EstimateDataRecoveryTimeCommand.class);
      doReturn(p).when(newCmd).getParams();
      doReturn(p.getPCtrlCliWithoutCluster()).when(newCmd).buildControllerClient(anyString(), anyString(), any());
      tasks.add(new DataRecoveryTask(newCmd, taskParams));
    }
    return tasks;
  }

  private List<DataRecoveryTask> buildMonitorDataRecoveryTasks(
      Set<String> storeNames,
      Command cmd,
      MonitorCommand.Params params) {
    List<DataRecoveryTask> tasks = new ArrayList<>();
    MonitorCommand.Params.Builder builder = new MonitorCommand.Params.Builder(params);
    for (String name: storeNames) {
      MonitorCommand.Params p = builder.build();
      p.setStore(name);
      DataRecoveryTask.TaskParams taskParams = new DataRecoveryTask.TaskParams(name, p);
      MonitorCommand newCmd = spy(MonitorCommand.class);
      doReturn(p).when(newCmd).getParams();
      doReturn(p.getPCtrlCliWithoutCluster()).when(newCmd).buildControllerClient(anyString(), anyString(), any());
      tasks.add(new DataRecoveryTask(newCmd, taskParams));
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

    MonitorCommand.Params.Builder builder = new MonitorCommand.Params.Builder().setTargetRegion(region)
        .setDateTime(now)
        .setPCtrlCliWithoutCluster(mockedCli)
        .setParentUrl("https://localhost:7036")
        .setSSLFactory(Optional.empty());
    MonitorCommand.Params monitorParams = builder.build();

    MonitorCommand mockMonitorCmd = spy(MonitorCommand.class);
    mockMonitorCmd.setParams(monitorParams);
    mockMonitorCmd.setOngoingOfflinePushDetected(false);
    doReturn(mockedCli).when(mockMonitorCmd).buildControllerClient(anyString(), anyString(), any());

    doReturn(monitorParams).when(mockMonitorCmd).getParams();

    // Inject the mocked command into the running system.
    Set<String> storeName = new HashSet<>(Arrays.asList("store1", "store2", "store3"));
    List<DataRecoveryTask> tasks = buildMonitorDataRecoveryTasks(storeName, mockMonitorCmd, monitorParams);
    doReturn(tasks).when(monitor).buildTasks(any(), any());

    // client monitors three store recovery progress.
    DataRecoveryClient.DataRecoveryParams drParams = new DataRecoveryClient.DataRecoveryParams(storeName);
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

    if (status.isError()) {
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

    if (status.isError()) {
      // Verify all stores are in error state.
      for (int i = 0; i < numOfStores; i++) {
        Assert.assertTrue(monitor.getTasks().get(i).getTaskResult().isError());
        Assert.assertNull(monitor.getTasks().get(i).getTaskResult().getMessage());
      }
    }

    // duplicate check

    Set<String> storeNames = new HashSet<>();
    for (int i = 0; i < numOfStores; i++) {
      Assert.assertFalse(storeNames.contains(monitor.getTasks().get(i).getTaskParams().getStore()));
      storeNames.add(monitor.getTasks().get(i).getTaskParams().getStore());
    }
  }

  @Test
  public void testRepushViabilityInfo() {
    RepushViabilityInfo info = new RepushViabilityInfo();
    info.viableWithResult(RepushViabilityInfo.Result.SUCCESS);
    Assert.assertFalse(info.isError());

    info.inViableWithResult(RepushViabilityInfo.Result.CURRENT_VERSION_IS_NEWER);
    Assert.assertFalse(info.isError());

    info.inViableWithError(RepushViabilityInfo.Result.DISCOVERY_ERROR);
    Assert.assertTrue(info.isError());
  }

  @Test
  public void testRepushCommandExecute() {
    StoreRepushCommand repushCommand = spy(StoreRepushCommand.class);
    RepushViabilityInfo info = new RepushViabilityInfo();
    info.inViableWithResult(RepushViabilityInfo.Result.CURRENT_VERSION_IS_NEWER);
    StoreRepushCommand.Params repushParams = new StoreRepushCommand.Params();
    doReturn(info).when(repushCommand).getRepushViability();
    doReturn(repushParams).when(repushCommand).getParams();

    repushCommand.execute();
    Assert.assertFalse(repushCommand.getResult().isError());

    info.inViableWithError(RepushViabilityInfo.Result.DISCOVERY_ERROR);
    repushCommand.execute();
    Assert.assertTrue(repushCommand.getResult().isError());
  }

}
