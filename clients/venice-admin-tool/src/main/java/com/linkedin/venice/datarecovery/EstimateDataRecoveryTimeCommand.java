package com.linkedin.venice.datarecovery;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.StoreHealthAuditResponse;
import com.linkedin.venice.meta.RegionPushDetails;
import com.linkedin.venice.security.SSLFactory;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Optional;


public class EstimateDataRecoveryTimeCommand extends Command {
  private Params params;
  private EstimateDataRecoveryTimeCommand.Result result = new EstimateDataRecoveryTimeCommand.Result();

  public EstimateDataRecoveryTimeCommand(Params params) {
    this.params = params;
  }

  // for mockito
  public EstimateDataRecoveryTimeCommand() {
    this.params = null;
  }

  public Params getParams() {
    return params;
  }

  public void setParams(Params params) {
    this.params = params;
  }

  @Override
  public EstimateDataRecoveryTimeCommand.Result getResult() {
    return result;
  }

  @Override
  public boolean needWaitForFirstTaskToComplete() {
    return false;
  }

  public void setResult(Result result) {
    this.result = result;
  }

  public ControllerClient buildControllerClient(
      String clusterName,
      String discoveryUrls,
      Optional<SSLFactory> sslFactory) {
    return new ControllerClient(clusterName, discoveryUrls, sslFactory);
  }

  @Override
  public void execute() {
    // get store's push + partition info
    String storeName = getParams().getStore();
    String clusterName = getParams().getPCtrlCliWithoutCluster().discoverCluster(storeName).getCluster();

    try (ControllerClient parentCtrlCli =
        buildControllerClient(clusterName, getParams().getParentUrl(), getParams().getSslFactory())) {
      StoreHealthAuditResponse storeHealthInfo = parentCtrlCli.listStorePushInfo(getParams().getStore(), true);
      Map<String, RegionPushDetails> pushDetails = storeHealthInfo.getRegionPushDetails();

      if (pushDetails.containsKey(getParams().getTargetRegion())) {
        LocalDateTime startTime =
            LocalDateTime.parse(pushDetails.get(getParams().getTargetRegion()).getPushStartTimestamp());
        LocalDateTime endTime =
            LocalDateTime.parse(pushDetails.get(getParams().getTargetRegion()).getPushEndTimestamp());
        this.setResult(new EstimateDataRecoveryTimeCommand.Result(startTime.until(endTime, ChronoUnit.SECONDS)));
      } else {
        this.setResult(new EstimateDataRecoveryTimeCommand.Result("target region not found"));
      }
    } catch (Error e) {
      this.setResult(new EstimateDataRecoveryTimeCommand.Result("unable to create controller client: " + e));
    }
    this.getResult().setCoreWorkDone(true);
  }

  public static class Params extends Command.Params {
    private String targetRegion;
    private ControllerClient pCtrlCliWithoutCluster;
    private String parentUrl;
    private Optional<SSLFactory> sslFactory;

    public Params(Params p) {
      setStore(p.getStore());
      setPCtrlCliWithoutCluster(p.getPCtrlCliWithoutCluster());
      setTargetRegion(p.getTargetRegion());
      setSslFactory(p.getSslFactory());
      setParentUrl((p.getParentUrl()));
    }

    public Params() {
    }

    public String getTargetRegion() {
      return targetRegion;
    }

    public void setTargetRegion(String targetRegion) {
      this.targetRegion = targetRegion;
    }

    public ControllerClient getPCtrlCliWithoutCluster() {
      return pCtrlCliWithoutCluster;
    }

    public void setPCtrlCliWithoutCluster(ControllerClient pCtrlCliWithoutCluster) {
      this.pCtrlCliWithoutCluster = pCtrlCliWithoutCluster;
    }

    public Optional<SSLFactory> getSslFactory() {
      return sslFactory;
    }

    public void setSslFactory(Optional<SSLFactory> sslFactory) {
      this.sslFactory = sslFactory;
    }

    public String getParentUrl() {
      return parentUrl;
    }

    public void setParentUrl(String parentUrl) {
      this.parentUrl = parentUrl;
    }
  }

  public static class Result extends Command.Result {
    private Long estimatedRecoveryTimeInSeconds;
    private String message;

    public Result() {
      this.estimatedRecoveryTimeInSeconds = 0L;
      this.message = "task in progress";
    }

    public Result(Long timeInSeconds) {
      this.estimatedRecoveryTimeInSeconds = timeInSeconds;
      this.message = "EstimateDataRecoveryTimeCommand.Result: " + timeInSeconds;
    }

    public Result(String errorMessage) {
      this.estimatedRecoveryTimeInSeconds = -1L;
      this.setError(errorMessage);
      this.message = errorMessage;
    }

    public String getTimestamp() {
      if (this.estimatedRecoveryTimeInSeconds <= 0) {
        return "00:00:00";
      } else {
        int hours = (int) (this.estimatedRecoveryTimeInSeconds / 3600);
        int minutes = (int) ((this.estimatedRecoveryTimeInSeconds % 3600) / 60);
        int seconds = (int) (this.estimatedRecoveryTimeInSeconds % 60);

        return (String.format("%02d:%02d:%02d", hours, minutes, seconds));
      }
    }

    public Long getEstimatedRecoveryTimeInSeconds() {
      return estimatedRecoveryTimeInSeconds;
    }

    public void setEstimatedRecoveryTimeInSeconds(Long estimatedRecoveryTimeInSeconds) {
      this.estimatedRecoveryTimeInSeconds = estimatedRecoveryTimeInSeconds;
    }

    public String getMessage() {
      return message;
    }

    public void setMessage(String message) {
      this.message = message;
    }
  }
}
