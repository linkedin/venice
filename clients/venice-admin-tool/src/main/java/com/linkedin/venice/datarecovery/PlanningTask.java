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
  private final TaskParams _params;
  private Integer estimatedTimeResult = -2;
  private ControllerClient controllerClient;

  public PlanningTask(TaskParams params) {
    this._params = params;
    this.controllerClient = params.getCmdParams().getPCtrlCliWithoutCluster();
  }

  @Override
  public void run() {
    estimatedTimeResult = 0;

    // get store's push + partition info
    StoreHealthAuditResponse storeHealthInfo = controllerClient.listStorePushInfo(_params.getStoreName(), true);
    Map<String, RegionPushDetails> pushDetails = storeHealthInfo.getRegionPushDetails();

    if (pushDetails.containsKey(getParams().getCmdParams().getTargetRegion())) {
      Instant startTime =
          Instant.parse(pushDetails.get(getParams().getCmdParams().getTargetRegion()).getPushStartTimestamp() + "Z");
      Instant endTime =
          Instant.parse(pushDetails.get(getParams().getCmdParams().getTargetRegion()).getPushEndTimestamp() + "Z");
      estimatedTimeResult = (int) startTime.until(endTime, ChronoUnit.SECONDS);
    } else {
      estimatedTimeResult = -1;
    }
  }

  public TaskParams getParams() {
    return _params;
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
    private final EstimateDataRecoveryTimeCommand.Params cmdParams;

    public TaskParams(String storeName, EstimateDataRecoveryTimeCommand.Params cmdParams) {
      this.storeName = storeName;
      this.cmdParams = cmdParams;
    }

    public String getStoreName() {
      return storeName;
    }

    public EstimateDataRecoveryTimeCommand.Params getCmdParams() {
      return cmdParams;
    }

  }
}
