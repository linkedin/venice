package com.linkedin.venice.controllerapi;

import com.linkedin.venice.LastSucceedExecutionIdResponse;
import java.util.HashSet;
import java.util.Set;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestAdminCommandExecution {
  private AdminCommandExecution execution;
  private long id = 1;
  private Set<String> fabrics = new HashSet<>();
  private String fabric1 = "TestAdminCommandExecution1";
  private String fabric2 = "TestAdminCommandExecution2";
  private ControllerClient mockControllerClient;
  private LastSucceedExecutionIdResponse mockResponse;

  @BeforeMethod
  public void setUp() {
    fabrics.add(fabric1);
    fabrics.add(fabric2);
    mockControllerClient = Mockito.mock(ControllerClient.class);
    mockResponse = new LastSucceedExecutionIdResponse();
    Mockito.doReturn(mockResponse).when(mockControllerClient).getLastSucceedExecutionId();
    execution = new AdminCommandExecution(id, "test", "test", fabrics);
  }

  @Test
  public void testIsSucceedInAllFabric() {
    Assert.assertFalse(execution.isSucceedInAllFabric(), "Command haven't been send to remote fabrics.");
    execution.updateCommandStatusForFabric(fabric1, AdminCommandExecutionStatus.COMPLETED);
    execution.updateCommandStatusForFabric(fabric2, AdminCommandExecutionStatus.PROCESSING);
    Assert.assertFalse(execution.isSucceedInAllFabric(), "Command haven't been executed in " + fabric2);

    execution.updateCommandStatusForFabric(fabric2, AdminCommandExecutionStatus.COMPLETED);
    Assert.assertTrue(execution.isSucceedInAllFabric(), "Command has succeed in all fabrics.");
  }

  @Test
  public void testCheckStatusInRemoteFabric() {
    mockResponse.setLastSucceedExecutionId(id);
    execution.checkAndUpdateStatusForRemoteFabric(fabric1, mockControllerClient);
    Assert.assertFalse(execution.isSucceedInAllFabric(), "Haven't checked status in fabric2");
    execution.checkAndUpdateStatusForRemoteFabric(fabric2, mockControllerClient);
    Assert.assertTrue(execution.isSucceedInAllFabric(), "Have checked all fabrics. All of them returned completed.");
  }

  @Test
  public void testCheckStatusInRemoteFabricProcessing() {
    mockResponse.setLastSucceedExecutionId(0);
    execution.checkAndUpdateStatusForRemoteFabric(fabric1, mockControllerClient);
    Assert.assertEquals(execution.getFabricToExecutionStatusMap().get(fabric1), AdminCommandExecutionStatus.PROCESSING);
  }

  @Test
  public void testCheckStatusInRemoteFabricError() {
    mockResponse.setError("Test error");
    execution.checkAndUpdateStatusForRemoteFabric(fabric1, mockControllerClient);
    Assert.assertEquals(execution.getFabricToExecutionStatusMap().get(fabric1), AdminCommandExecutionStatus.UNKNOWN);
  }
}
