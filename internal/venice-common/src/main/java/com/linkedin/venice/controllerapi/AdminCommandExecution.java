package com.linkedin.venice.controllerapi;

import com.linkedin.venice.LastSucceedExecutionIdResponse;
import com.linkedin.venice.exceptions.VeniceException;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * The execution object is a kind of context for a admin command including essential information used to track the
 * status of this command.
 */
public class AdminCommandExecution {
  private long executionId;
  private String operation;
  private String clusterName;
  private String startTime;
  /**
   * Execution status in remote fabrics.
   */
  private ConcurrentMap<String, AdminCommandExecutionStatus> fabricToExecutionStatusMap;

  private static final Logger LOGGER = LogManager.getLogger(AdminCommandExecution.class);

  /**
   * CTOR used by JSON serializer.
   */
  public AdminCommandExecution() {
  }

  public AdminCommandExecution(long executionId, String operation, String clusterName, Collection<String> fabrics) {
    this.executionId = executionId;
    this.operation = operation;
    this.clusterName = clusterName;
    this.startTime = LocalDateTime.now().toString();
    this.fabricToExecutionStatusMap = new ConcurrentHashMap<>();
    if (fabrics.isEmpty()) {
      throw new VeniceException("At least one remote fabric is required.");
    }
    for (String fabric: fabrics) {
      this.fabricToExecutionStatusMap.put(fabric, AdminCommandExecutionStatus.NOT_SENT);
    }
  }

  public boolean isSucceedInAllFabric() {
    for (AdminCommandExecutionStatus status: fabricToExecutionStatusMap.values()) {
      if (!status.equals(AdminCommandExecutionStatus.COMPLETED)) {
        return false;
      }
    }
    return true;
  }

  public void checkAndUpdateStatusForRemoteFabric(String fabric, ControllerClient controllerClient) {
    try {
      if (fabricToExecutionStatusMap.get(fabric).equals(AdminCommandExecutionStatus.COMPLETED)) {
        return;
      }
      LastSucceedExecutionIdResponse response = controllerClient.getLastSucceedExecutionId();
      if (response.isError()) {
        throw new VeniceException(
            "Query the last succeed execution id from fabric: " + fabric + " failed. Caused by: "
                + response.getError());
      } else if (this.executionId <= response.getLastSucceedExecutionId()) {
        // Command has been processed in remote fabric.
        this.updateCommandStatusForFabric(fabric, AdminCommandExecutionStatus.COMPLETED);
      } else {
        this.updateCommandStatusForFabric(fabric, AdminCommandExecutionStatus.PROCESSING);
      }
    } catch (Exception e) {
      LOGGER.error("Can not get status from fabric: {}", fabric, e);
      this.updateCommandStatusForFabric(fabric, AdminCommandExecutionStatus.UNKNOWN);
    }
  }

  public void updateCommandStatusForFabric(String fabric, AdminCommandExecutionStatus status) {
    fabricToExecutionStatusMap.put(fabric, status);
  }

  public long getExecutionId() {
    return executionId;
  }

  public void setExecutionId(long executionId) {
    this.executionId = executionId;
  }

  public String getOperation() {
    return operation;
  }

  public void setOperation(String operation) {
    this.operation = operation;
  }

  public String getClusterName() {
    return clusterName;
  }

  public void setClusterName(String clusterName) {
    this.clusterName = clusterName;
  }

  public String getStartTime() {
    return startTime;
  }

  public void setStartTime(String startTime) {
    this.startTime = startTime;
  }

  public ConcurrentMap<String, AdminCommandExecutionStatus> getFabricToExecutionStatusMap() {
    return fabricToExecutionStatusMap;
  }

  public void setFabricToExecutionStatusMap(
      ConcurrentMap<String, AdminCommandExecutionStatus> fabricToExecutionStatusMap) {
    this.fabricToExecutionStatusMap = fabricToExecutionStatusMap;
  }

  @Override
  public String toString() {
    return "AdminCommandExecution{" + "executionId=" + executionId + ", operation='" + operation + '\''
        + ", clusterName='" + clusterName + '\'' + ", startTime='" + startTime + '\'' + ", fabricToExecutionStatusMap="
        + fabricToExecutionStatusMap + '}';
  }
}
