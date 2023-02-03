package com.linkedin.venice;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import com.linkedin.venice.datarecovery.Client;
import com.linkedin.venice.datarecovery.Module;
import com.linkedin.venice.datarecovery.StoreRepushCommand;
import com.linkedin.venice.datarecovery.Task;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestDataRecoveryClient {
  private Module executor;
  private Client client;

  @Test
  public void testExecutor() {
    for (boolean isSuccess: new boolean[] { true, false }) {
      executeRecovery(isSuccess);
      verifyRecoveryResults(isSuccess);
    }
  }

  private void verifyRecoveryResults(boolean isSuccess) {
    Assert.assertEquals(executor.getTasks().size(), 3);
    if (isSuccess) {
      // Verify all stores are executed successfully.
      Assert.assertFalse(executor.getTasks().get(0).getTaskResult().isError());
      Assert.assertFalse(executor.getTasks().get(1).getTaskResult().isError());
      Assert.assertFalse(executor.getTasks().get(2).getTaskResult().isError());
    } else {
      // Verify all stores are executed successfully.
      Assert.assertTrue(executor.getTasks().get(0).getTaskResult().isError());
      Assert.assertTrue(executor.getTasks().get(1).getTaskResult().isError());
      Assert.assertTrue(executor.getTasks().get(2).getTaskResult().isError());
    }
  }

  private void executeRecovery(boolean isSuccess) {
    StoreRepushCommand.Params cmdParams = new StoreRepushCommand.Params();
    cmdParams.setFabricGroup("ei");

    // Partial mock of Module class to take password from console input.
    executor = spy(Module.class);
    doReturn("test").when(executor).getPasswordVIP();

    // Mock command to mimic a successful repush result.
    List<String> mockCmd = new ArrayList<>();
    mockCmd.add("sh");
    mockCmd.add("-c");

    if (isSuccess) {
      mockCmd.add("echo \"https://ltx1-faroaz01.grid.linkedin.com:8443/executor?execid=21585379\"");
    } else {
      mockCmd.add(
          "echo \"Could not fetch session information from Azkaban. Response: {'error': 'Incorrect Login. Username/Password+VIP not found.'}\"");
    }
    StoreRepushCommand mockStoreRepushCmd = spy(StoreRepushCommand.class);
    mockStoreRepushCmd.setParams(cmdParams);
    doReturn(mockCmd).when(mockStoreRepushCmd).getExpectCmd();

    // Inject the mocked command into the running system.
    Set<String> storeName = new HashSet<>(Arrays.asList("store1", "store2", "store3"));
    List<Task> tasks = buildTasks(storeName, mockStoreRepushCmd, cmdParams);
    doReturn(tasks).when(executor).buildTasks(any(), any());

    // Partial mock of Client class to confirm to-be-repushed stores from standard input.
    client = mock(Client.class);
    doReturn(executor).when(client).getExecutor();
    doCallRealMethod().when(client).execute(any(), any());
    doReturn(true).when(client).confirmStores(any());
    // client executes three store recovery.
    client.execute(client.new OperationLevel("store1,store2,store3"), cmdParams);
  }

  private List<Task> buildTasks(Set<String> storeNames, StoreRepushCommand cmd, StoreRepushCommand.Params params) {
    List<Task> tasks = new ArrayList<>();
    for (String name: storeNames) {
      Task.TaskParams taskParams = new Task.TaskParams(name, params);
      tasks.add(new Task(cmd, taskParams));
    }
    return tasks;
  }
}
