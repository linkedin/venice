package com.linkedin.venice.datarecovery;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.StoreHealthAuditResponse;
import com.linkedin.venice.meta.RegionPushDetails;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Map;


/**
 * A Task is wrapper class that designed to execute multiple commands in sequence.
 */
public class PlanningTask implements Runnable {
  private final TaskParams params;
  private EstimateDataRecoveryTimeCommand.Result result;
  private ControllerClient controllerClient;

  public PlanningTask(TaskParams params) {
    this.params = params;
    this.controllerClient = params.getCmdParams().getPCtrlCliWithoutCluster();
    this.result = null;
  }

  @Override
  public void run() {
    // get store's push + partition info
    StoreHealthAuditResponse storeHealthInfo = controllerClient.listStorePushInfo(params.getStoreName(), true);
    Map<String, RegionPushDetails> pushDetails = storeHealthInfo.getRegionPushDetails();

    if (pushDetails.containsKey(getParams().getCmdParams().getTargetRegion())) {
      LocalDateTime startTime =
          LocalDateTime.parse(pushDetails.get(getParams().getCmdParams().getTargetRegion()).getPushStartTimestamp());
      LocalDateTime endTime =
          LocalDateTime.parse(pushDetails.get(getParams().getCmdParams().getTargetRegion()).getPushEndTimestamp());
      this.setResult(new EstimateDataRecoveryTimeCommand.Result(startTime.until(endTime, ChronoUnit.SECONDS)));
    } else {
      this.setResult(new EstimateDataRecoveryTimeCommand.Result("target region not found"));
    }
  }

  public TaskParams getParams() {
    return params;
  }

  public EstimateDataRecoveryTimeCommand.Result getResult() {
    return result;
  }

  public void setResult(EstimateDataRecoveryTimeCommand.Result estimatedTimeResult) {
    this.result = estimatedTimeResult;
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
