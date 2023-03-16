package com.linkedin.venice.datarecovery;

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
  public static class Params {
    private String clusterName;
    private String parentUrl;
    private boolean debug = false;

    public String getClusterName() {
      return clusterName;
    }

    public void setClusterName(String clusterName) {
      this.clusterName = clusterName;
    }

    public String getParentUrl() {
      return parentUrl;
    }

    public void setParentUrl(String parentUrl) {
      this.parentUrl = parentUrl;
    }

    public void setDebug(boolean debug) {
      this.debug = debug;
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
