package com.linkedin.venice;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.StoreHealthAuditResponse;
import com.linkedin.venice.datarecovery.DataRecoveryClient;
import com.linkedin.venice.datarecovery.DataRecoveryExecutor;
import com.linkedin.venice.datarecovery.DataRecoveryTask;
import com.linkedin.venice.datarecovery.EstimateDataRecoveryTimeCommand;
import com.linkedin.venice.datarecovery.Estimator;
import com.linkedin.venice.datarecovery.PlanningTask;
import com.linkedin.venice.datarecovery.StoreRepushCommand;
import com.linkedin.venice.meta.RegionPushDetails;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestDataRecoveryClient {
  private DataRecoveryExecutor executor;
  private Estimator estimator;
  private ControllerClient controllerClient;

  @Test
  public void testMonitor() {
    estimateRecovery();
    verifyEstimationResults();
  }

  @Test
  public void testExecutor() {
    for (boolean isSuccess: new boolean[] { true, false }) {
      executeRecovery(isSuccess);
      verifyRecoveryResults(isSuccess);
    }
  }

  private void verifyEstimationResults() {
    Long expectedRecoveryTime = 7200L;
    Long result = 0L;
    for (PlanningTask t: estimator.getTasks()) {
      result += t.getResult().getEstimatedRecoveryTimeInSeconds();
    }
    Assert.assertEquals(result, expectedRecoveryTime);
  }

  private void verifyRecoveryResults(boolean isSuccess) {
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
    estimator = spy(Estimator.class);
    controllerClient = mock(ControllerClient.class);

    Set<String> storeNames = new HashSet<>(Arrays.asList("store1", "store2"));
    EstimateDataRecoveryTimeCommand.Params cmdParams = new EstimateDataRecoveryTimeCommand.Params();
    cmdParams.setTargetRegion("region1");
    cmdParams.setParentUrl("https://localhost:7036");
    cmdParams.setPCtrlCliWithoutCluster(controllerClient);
    List<PlanningTask> tasks = buildPlanningTasks(storeNames, cmdParams);

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
    doReturn("testcluster").when(controllerClient).getClusterName();

    dataRecoveryClient
        .estimateRecoveryTime(new DataRecoveryClient.DataRecoveryParams("store1,store2", true), cmdParams);
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
    dataRecoveryClient.execute(new DataRecoveryClient.DataRecoveryParams("store1,store2,store3", true), cmdParams);
  }

  private List<DataRecoveryTask> buildTasks(
      Set<String> storeNames,
      StoreRepushCommand cmd,
      StoreRepushCommand.Params params) {
    List<DataRecoveryTask> tasks = new ArrayList<>();
    for (String name: storeNames) {
      DataRecoveryTask.TaskParams taskParams = new DataRecoveryTask.TaskParams(name, params);
      tasks.add(new DataRecoveryTask(cmd, taskParams));
    }
    return tasks;
  }

  private List<PlanningTask> buildPlanningTasks(Set<String> storeNames, EstimateDataRecoveryTimeCommand.Params params) {
    List<PlanningTask> tasks = new ArrayList<>();
    for (String name: storeNames) {
      PlanningTask.TaskParams taskParams = new PlanningTask.TaskParams(name, params);
      tasks.add(new PlanningTask(taskParams));
    }
    return tasks;
  }
}
