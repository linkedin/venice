package com.linkedin.venice.datarecovery;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.security.SSLFactory;
import java.util.Optional;


/**
 * EstimateRecoveryTimeCommand contains the details of a request for estimating the recovery time of a store or set of stores.
 * We expect the command to comply with the following contract:
 *
 * Input:
 *    admin-tool.sh --list-store-push-info --url <url> --stores <store_names> --fabric <source_fabric>
 * Output:
 *    success: {store, estimated_recovery_time}
 *    failure: {store, error}
 */

public class EstimateDataRecoveryTimeCommand {
  private String storeName;
  private Params params;

  public Params getParams() {
    return params;
  }

  public void setParams(Params params) {
    this.params = params;
  }

  public String getStoreName() {
    return storeName;
  }

  public void setStoreName(String storeName) {
    this.storeName = storeName;
  }

  public static class Params {
    private String targetRegion;
    private ControllerClient pCtrlCliWithoutCluster;
    private String parentUrl;
    private Optional<SSLFactory> sslFactory;

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

  public static class Result {
    private Long estimatedRecoveryTimeInSeconds;
    private boolean error;
    private String message;

    public Result() {
      this.estimatedRecoveryTimeInSeconds = 0L;
      this.error = false;
      this.message = "task in progress";
    }

    public Result(Long timeInSeconds) {
      this.estimatedRecoveryTimeInSeconds = timeInSeconds;
      this.error = false;
      this.message = "EstimateDataRecoveryTimeCommand.Result: " + (timeInSeconds / 3600) + ":" + (timeInSeconds % 60)
          + ":" + (timeInSeconds % 10);
    }

    public Result(String errorMessage) {
      this.estimatedRecoveryTimeInSeconds = -1L;
      this.error = true;
      this.message = errorMessage;
    }

    @Override
    public String toString() {
      return this.message;
    }

    public boolean isError() {
      return this.error;
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
