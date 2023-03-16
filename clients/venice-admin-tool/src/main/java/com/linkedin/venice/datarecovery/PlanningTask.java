package com.linkedin.venice.datarecovery;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.StoreHealthAuditResponse;
import com.linkedin.venice.meta.RegionPushDetails;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;


/**
 * A Task is wrapper class that designed to execute multiple commands in sequence.
 */
public class PlanningTask implements Runnable {
  private final TaskParams taskParams;
  private Integer estimatedTimeResult = -2;
  private ControllerClient controllerClient;

  public PlanningTask(TaskParams params, ControllerClient controllerClient) {
    this.taskParams = params;
    this.controllerClient = controllerClient;
  }

  @Override
  public void run() {
    estimatedTimeResult = 0;
    // get store's push + partition info
    StoreHealthAuditResponse storeHealthInfo = controllerClient.listStorePushInfo(taskParams.getStoreName(), true);
    Map<String, RegionPushDetails> pushDetails = storeHealthInfo.getRegionPushDetails();
    int ret = 0;

    // examine and record/avg push times
    for (Map.Entry<String, RegionPushDetails> entry: pushDetails.entrySet()) {
      Instant startTime = Instant.parse(entry.getValue().getPushStartTimestamp());
      Instant endTime = Instant.parse(entry.getValue().getPushEndTimestamp());
      ret += startTime.until(endTime, ChronoUnit.SECONDS);
    }

    estimatedTimeResult = ret;
  }

  public TaskParams getTaskParams() {
    return taskParams;
  }

  public Integer getEstimatedTimeResult() {
    return estimatedTimeResult;
  }

  public void setEstimatedTimeResult(Integer estimatedTimeResult) {
    this.estimatedTimeResult = estimatedTimeResult;
  }

  public static class TaskParams {
    // Store name.
    private final String storeName;
    private String clusterName;

    public TaskParams(String clusterName, String storeName) {
      this.storeName = storeName;
      this.clusterName = clusterName;
    }

    public String getStoreName() {
      return storeName;
    }

    public String getClusterName() {
      return clusterName;
    }
  }
}
