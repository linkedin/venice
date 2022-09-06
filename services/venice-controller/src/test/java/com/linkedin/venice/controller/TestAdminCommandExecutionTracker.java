package com.linkedin.venice.controller;

import com.linkedin.venice.LastSucceedExecutionIdResponse;
import com.linkedin.venice.admin.InMemoryExecutionIdAccessor;
import com.linkedin.venice.controllerapi.AdminCommandExecution;
import com.linkedin.venice.controllerapi.AdminCommandExecutionStatus;
import com.linkedin.venice.controllerapi.ControllerClient;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestAdminCommandExecutionTracker {
  private AdminCommandExecutionTracker executionTracker;
  private int executionTTLHour = 1;
  private String fabric1 = "TestAdminCommandExecutionTracker1";
  private String fabric2 = "TestAdminCommandExecutionTracker2";
  private String cluster = "TestAdminCommandExecutionTracker";
  private Map<String, ControllerClient> fabricToControllerMap;
  private ControllerClient mockControllerClient;
  private LastSucceedExecutionIdResponse mockResponse;
  private InMemoryExecutionIdAccessor accessor;

  @BeforeMethod
  public void setUp() {
    fabricToControllerMap = new HashMap<>();
    mockControllerClient = Mockito.mock(ControllerClient.class);
    mockResponse = new LastSucceedExecutionIdResponse();
    Mockito.doReturn(mockResponse).when(mockControllerClient).getLastSucceedExecutionId();
    fabricToControllerMap.put(fabric1, mockControllerClient);
    fabricToControllerMap.put(fabric2, mockControllerClient);
    accessor = new InMemoryExecutionIdAccessor();
    executionTracker = new AdminCommandExecutionTracker(cluster, accessor, fabricToControllerMap, executionTTLHour);
  }

  @Test
  public void testCreateExecution() {
    String operation = "testOperation";
    AdminCommandExecution execution = executionTracker.createExecution(operation);

    Assert.assertEquals(execution.getClusterName(), cluster);
    Assert.assertEquals(execution.getOperation(), operation);
    Assert.assertEquals(execution.getFabricToExecutionStatusMap().size(), fabricToControllerMap.size());
    LocalDateTime startTime = LocalDateTime.parse(execution.getStartTime());
    Assert.assertTrue(startTime.compareTo(LocalDateTime.now()) <= 0, "Command should already be started.");
  }

  @Test
  public void testStartTrackingExecution() {
    String operation = "testOperation";
    int executionCount = 3;
    Set<Long> idSet = new HashSet<>();
    for (int i = 0; i < executionCount; i++) {
      AdminCommandExecution execution = executionTracker.createExecution(operation);
      idSet.add(execution.getExecutionId());
      executionTracker.startTrackingExecution(execution);
    }
    Assert.assertEquals(idSet.size(), executionCount, "Generated ID should be unique.");
    for (Long id: idSet) {
      Assert.assertNotNull(executionTracker.getExecution(id), "Execution should not be expired.");
    }
  }

  @Test
  public void testExpireExecution() {
    String operation = "testOperation";
    int expiredExecutionCount = 3;
    List<AdminCommandExecution> executions = new ArrayList<>();
    for (int i = 0; i < expiredExecutionCount; i++) {
      AdminCommandExecution execution = executionTracker.createExecution(operation);
      executionTracker.startTrackingExecution(execution);
      executions.add(execution);
    }

    for (AdminCommandExecution execution: executions) {
      Assert
          .assertNotNull(executionTracker.getExecution(execution.getExecutionId()), "Execution should not be expired.");
      // set start time to 2 days ago.
      execution.setStartTime(LocalDateTime.now().minusDays(2).toString());
      for (String fabric: fabricToControllerMap.keySet()) {
        execution.updateCommandStatusForFabric(fabric, AdminCommandExecutionStatus.COMPLETED);
      }
    }

    // Execution should not be expired until a new execution is added.
    for (AdminCommandExecution execution: executions) {
      Assert
          .assertNotNull(executionTracker.getExecution(execution.getExecutionId()), "Execution should not be expired.");
    }

    AdminCommandExecution oldButUncompletedExecution = executionTracker.createExecution(operation);
    oldButUncompletedExecution.setStartTime(LocalDateTime.now().minusDays(2).toString());
    executionTracker.startTrackingExecution(oldButUncompletedExecution);
    for (AdminCommandExecution execution: executions) {
      Assert.assertNull(executionTracker.getExecution(execution.getExecutionId()), "Execution should be expired.");
    }

    AdminCommandExecution newExecution = executionTracker.createExecution(operation);
    executionTracker.startTrackingExecution(newExecution);
    Assert.assertNotNull(
        executionTracker.getExecution(oldButUncompletedExecution.getExecutionId()),
        "Execution should not be expired.");
    Assert.assertNotNull(
        executionTracker.getExecution(newExecution.getExecutionId()),
        "Execution should not be expired.");
  }

  @Test
  public void testCheckExecutionStatus() {
    AdminCommandExecution execution = executionTracker.createExecution("test");
    executionTracker.startTrackingExecution(execution);

    Assert.assertNull(executionTracker.checkExecutionStatus(-1), "Execution has not been created yet");
    mockResponse.setLastSucceedExecutionId(-1);
    execution = executionTracker.checkExecutionStatus(execution.getExecutionId());
    Assert.assertFalse(execution.isSucceedInAllFabric(), "Command has not been completed.");
    // Processing command in all remote fabrics.
    for (AdminCommandExecutionStatus status: execution.getFabricToExecutionStatusMap().values()) {
      Assert.assertEquals(status, AdminCommandExecutionStatus.PROCESSING);
    }

    mockResponse.setLastSucceedExecutionId(Long.MAX_VALUE);
    execution = executionTracker.checkExecutionStatus(execution.getExecutionId());
    Assert.assertTrue(execution.isSucceedInAllFabric(), "Command has been completed.");
  }

  @Test
  public void testCheckExecutionStatusError() {
    AdminCommandExecution execution = executionTracker.createExecution("test");
    executionTracker.startTrackingExecution(execution);
    mockResponse.setError("Test error");
    execution = executionTracker.checkExecutionStatus(execution.getExecutionId());
    Assert
        .assertFalse(execution.isSucceedInAllFabric(), "Query failed, could not know the status of remote execution.");
    for (AdminCommandExecutionStatus status: execution.getFabricToExecutionStatusMap().values()) {
      Assert.assertEquals(status, AdminCommandExecutionStatus.UNKNOWN);
    }
  }

  @Test
  public void testCreateTrackerWithInitialValue() {
    long initId = 100L;
    accessor.setExecutionId(initId);
    executionTracker = new AdminCommandExecutionTracker(cluster, accessor, fabricToControllerMap, executionTTLHour);
    Assert.assertEquals(executionTracker.getLastExecutionId(), initId);
  }
}
