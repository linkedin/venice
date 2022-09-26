package com.linkedin.venice.controller;

import com.linkedin.venice.controllerapi.AdminCommandExecution;
import com.linkedin.venice.controllerapi.ControllerClient;
import java.time.LocalDateTime;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class is used to track the execution of the async admin command. Async admin command is a kind of admin
 * command which is sent to a parent controller and propagated through a Kafka admin topic. Eventually command would be
 * executed by the controller in each PROD fabric.
 * <p>
 * The context of command execution would be stored in this tracker and expired in case it lives longer than TTL. It
 * also provides a way to check the latest execution status of given command by sending requests to remote fabric. But
 * the checking logic is defined in a closure when the command was created.
 * <p>
 * This class is Thread-safe.
 */
public class AdminCommandExecutionTracker {
  private static final int DEFAULT_TTL_HOUR = 24;
  private static final Logger LOGGER = LogManager.getLogger(AdminCommandExecutionTracker.class);

  private final String cluster;
  private final int executionTTLHour;
  private final LinkedHashMap<Long, AdminCommandExecution> idToExecutionMap;
  private final Map<String, ControllerClient> fabricToControllerClientsMap;
  private final ExecutionIdAccessor executionIdAccessor;

  public AdminCommandExecutionTracker(
      String cluster,
      ExecutionIdAccessor executionIdAccessor,
      Map<String, ControllerClient> fabricToControllerClientsMap,
      int executionTTLHour) {
    this.executionTTLHour = executionTTLHour;
    this.cluster = cluster;
    this.idToExecutionMap = new LinkedHashMap<>();
    this.fabricToControllerClientsMap = fabricToControllerClientsMap;
    this.executionIdAccessor = executionIdAccessor;
  }

  public AdminCommandExecutionTracker(
      String cluster,
      ExecutionIdAccessor executionIdAccessor,
      Map<String, ControllerClient> fabricToControllerClientsMap) {
    this(cluster, executionIdAccessor, fabricToControllerClientsMap, DEFAULT_TTL_HOUR);
  }

  /**
   * Create an execution context of a command.
   */
  public synchronized AdminCommandExecution createExecution(String operation) {
    return new AdminCommandExecution(
        getNextAvailableExecutionId(),
        operation,
        cluster,
        fabricToControllerClientsMap.keySet());
  }

  /**
   * Add execution context into local memory and expired old executions if needed.
   */
  public synchronized void startTrackingExecution(AdminCommandExecution execution) {
    idToExecutionMap.put(execution.getExecutionId(), execution);
    LOGGER.info(
        "Add Execution: {} for operation: {} into tracker.",
        execution.getExecutionId(),
        execution.getOperation());
    // Try to Collect executions which live longer than TTL.
    int collectedCount = 0;
    Iterator<AdminCommandExecution> iterator = idToExecutionMap.values().iterator();
    LocalDateTime earliestStartTimeToKeep = LocalDateTime.now().minusHours(executionTTLHour);
    while (iterator.hasNext()) {
      AdminCommandExecution oldExecution = iterator.next();
      LocalDateTime commandStartTime = LocalDateTime.parse(oldExecution.getStartTime());
      if (commandStartTime.isBefore(earliestStartTimeToKeep)) {
        // Only collect the execution which is started before the earliest start time to keep and already succeed.
        if (oldExecution.isSucceedInAllFabric()) {
          iterator.remove();
          collectedCount++;
        }
      } else {
        // Execution was started after the earliest start time to keep. Collection complete.
        break;
      }
    }
    LOGGER.info(
        "Collected {} executions which succeed and were executed before the earliest time to keep: {}",
        collectedCount,
        earliestStartTimeToKeep);
  }

  /**
   * Check the latest status of execution in remote fabrics.
   */
  public synchronized AdminCommandExecution checkExecutionStatus(long id) {
    AdminCommandExecution execution = idToExecutionMap.get(id);
    if (execution == null) {
      return null;
    }
    LOGGER.info("Sending query to remote fabrics to check status of execution: {}", id);
    for (Map.Entry<String, ControllerClient> entry: fabricToControllerClientsMap.entrySet()) {
      execution.checkAndUpdateStatusForRemoteFabric(entry.getKey(), entry.getValue());
    }
    LOGGER.info("Updated statuses in remote fabrics for execution: {}", id);
    return execution;
  }

  synchronized AdminCommandExecution getExecution(long id) {
    return idToExecutionMap.get(id);
  }

  private long getNextAvailableExecutionId() {
    return executionIdAccessor.incrementAndGetExecutionId(cluster);
  }

  public long getLastExecutionId() {
    return executionIdAccessor.getLastGeneratedExecutionId(cluster);
  }

  Map<String, ControllerClient> getFabricToControllerClientsMap() {
    return fabricToControllerClientsMap;
  }
}
