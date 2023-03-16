package com.linkedin.venice.datarecovery;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.security.SSLFactory;
import java.util.Optional;


/**
 * EstimateRecoveryTimeCommand contains the details of a request for estimating the recovery time of a store.
 * We expect the command to comply with the following contract:
 *
 * Input:
 *    admin-tool.sh --list-store-push-info --url <url>--store <store_name> --cluster <source_fabric>
 * Output:
 *    success: link_to_running_task
 *    failure: failure_reason
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
    private Integer estimatedRecoveryTimeInSeconds;
    private String error;
    private String message;

    public Integer getEstimatedRecoveryTimeInSeconds() {
      return estimatedRecoveryTimeInSeconds;
    }

    public void setEstimatedRecoveryTimeInSeconds(Integer estimatedRecoveryTimeInSeconds) {
      this.estimatedRecoveryTimeInSeconds = estimatedRecoveryTimeInSeconds;
    }

    public boolean isError() {
      return error != null;
    }

    public void setError(String error) {
      this.error = error;
    }

    public String getError() {
      return error;
    }

    public String getMessage() {
      return message;
    }

    public void setMessage(String message) {
      this.message = message;
    }
  }
}
