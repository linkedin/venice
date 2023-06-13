package com.linkedin.venice.datarecovery;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.controllerapi.MultiStoreStatusResponse;
import com.linkedin.venice.controllerapi.StoreHealthAuditResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.meta.RegionPushDetails;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.security.SSLFactory;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Optional;


public class MonitorCommand extends Command {
  private MonitorCommand.Params params;
  private final MonitorCommand.Result result = new MonitorCommand.Result();

  private boolean isOngoingOfflinePushDetected = false;

  // For unit test only.
  public MonitorCommand() {
  }

  public MonitorCommand(MonitorCommand.Params params) {
    this.params = params;
  }

  // For unit test only.
  public void setParams(MonitorCommand.Params params) {
    this.params = params;
  }

  public MonitorCommand.Params getParams() {
    return this.params;
  }

  // For unit test only.
  public void setOngoingOfflinePushDetected(boolean val) {
    this.isOngoingOfflinePushDetected = val;
  }

  @Override
  public MonitorCommand.Result getResult() {
    return result;
  }

  @Override
  public boolean needWaitForFirstTaskToComplete() {
    return false;
  }

  @Override
  public void execute() {
    String storeName = getParams().store;

    // Find out cluster name.
    String clusterName = getParams().getPCtrlCliWithoutCluster().discoverCluster(storeName).getCluster();

    // Create a new controller client with cluster name specified.
    try (ControllerClient parentCtrlCli =
        buildControllerClient(clusterName, getParams().parentUrl, getParams().sslFactory)) {
      StoreResponse storeResponse = parentCtrlCli.getStore(storeName);
      if (storeResponse.isError()) {
        completeCoreWorkWithError(storeResponse.getError());
        return;
      }

      result.setStoreInfo(storeResponse.getStore());

      if (!isOngoingOfflinePushDetected) {
        LocalDateTime currentVersionStartDateTime = retrieveCurrentVersionDateTime(parentCtrlCli);
        if (currentVersionStartDateTime.isAfter(getParams().dateTime)) {
          // If we have a current version for the given store started after the given date time, claim done.
          completeCoreWorkWithMessage(
              String.format(
                  "current ver is newer (%s) than date time (%s)",
                  currentVersionStartDateTime,
                  getParams().dateTime));
          return;
        }

        /*
         * Find out store future version. A store has a meaningful future version only when there is an ongoing
         * offline push for the store.
         */
        MultiStoreStatusResponse response = parentCtrlCli.getFutureVersions(clusterName, storeName);

        if (!response.getStoreStatusMap().containsKey(getParams().targetRegion)) {
          completeCoreWorkWithError(String.format("No status for region: %s", getParams().targetRegion));
          return;
        }

        int futureVersion = Integer.parseInt(response.getStoreStatusMap().get(getParams().targetRegion));
        if (futureVersion == Store.NON_EXISTING_VERSION) {
          result.setMessage(
              String.format(
                  "No ongoing offline pushes detected after given date time (%s), keep polling",
                  getParams().dateTime));
          return;
        }

        isOngoingOfflinePushDetected = true;
        String kafkaTopic = Version.composeKafkaTopic(storeName, futureVersion);
        result.setFutureVersion(futureVersion);
        result.setKafKaTopic(kafkaTopic);
      }

      // Query job status.
      JobStatusQueryResponse jobStatusQueryResponse =
          parentCtrlCli.queryDetailedJobStatus(result.kafKaTopic, getParams().targetRegion);

      if (jobStatusQueryResponse.isError()
          || jobStatusQueryResponse.getStatus().equalsIgnoreCase(ExecutionStatus.ERROR.toString())) {
        completeCoreWorkWithError(jobStatusQueryResponse.getStatusDetails());
        return;
      }

      if (jobStatusQueryResponse.getStatus().equalsIgnoreCase(ExecutionStatus.COMPLETED.toString())) {
        completeCoreWorkWithMessage(
            String.format(
                "ver: %d, status: %s",
                jobStatusQueryResponse.getVersion(),
                jobStatusQueryResponse.getStatus()));
        return;
      }
      // For other cases, report current status.
      result.setMessage(createReportMessage(jobStatusQueryResponse));
    }
  }

  // Retrieve the store's current version info.
  private LocalDateTime retrieveCurrentVersionDateTime(ControllerClient parentCtrlCli) {
    StoreHealthAuditResponse storeHealth = parentCtrlCli.listStorePushInfo(getParams().store, false);
    RegionPushDetails targetRegion = storeHealth.getRegionPushDetails().get(getParams().targetRegion);
    String dateTime = targetRegion.getPushStartTimestamp();
    return LocalDateTime.parse(dateTime, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
  }

  // A placeholder function for future improvement.
  private String createReportMessage(JobStatusQueryResponse resp) {
    return String.format(
        "ver: %d, status: %s, uncompleted ptn: %d/%d",
        resp.getVersion(),
        resp.getStatus(),
        resp.getUncompletedPartitions().size(),
        result.storeInfo.getPartitionCount());
  }

  private void completeCoreWorkWithError(String error) {
    result.setError(error);
    result.setCoreWorkDone(true);
  }

  private void completeCoreWorkWithMessage(String message) {
    result.setMessage(message);
    result.setCoreWorkDone(true);
  }

  public ControllerClient buildControllerClient(
      String clusterName,
      String discoveryUrls,
      Optional<SSLFactory> sslFactory) {
    return new ControllerClient(clusterName, discoveryUrls, sslFactory);
  }

  public static class Params extends Command.Params {
    // Target region.
    private String targetRegion;
    private ControllerClient pCtrlCliWithoutCluster;
    private String parentUrl;
    private Optional<SSLFactory> sslFactory;
    private LocalDateTime dateTime;

    public String getTargetRegion() {
      return targetRegion;
    }

    public ControllerClient getPCtrlCliWithoutCluster() {
      return pCtrlCliWithoutCluster;
    }

    public String getParentUrl() {
      return parentUrl;
    }

    public Optional<SSLFactory> getSSLFactory() {
      return sslFactory;
    }

    public LocalDateTime getDateTime() {
      return dateTime;
    }

    public static class Builder {
      private String targetRegion;
      private ControllerClient pCtrlCliWithoutCluster;
      private String parentUrl;
      private Optional<SSLFactory> sslFactory;
      private LocalDateTime dateTime;

      public Builder() {
      }

      public Builder(
          String targetRegion,
          ControllerClient controllerClient,
          String parentUrl,
          Optional<SSLFactory> sslFactory,
          LocalDateTime dateTime) {
        this.setTargetRegion(targetRegion)
            .setPCtrlCliWithoutCluster(controllerClient)
            .setParentUrl(parentUrl)
            .setSSLFactory(sslFactory)
            .setDateTime(dateTime);
      }

      public Builder(MonitorCommand.Params p) {
        this(p.targetRegion, p.pCtrlCliWithoutCluster, p.parentUrl, p.sslFactory, p.dateTime);
      }

      public MonitorCommand.Params build() {
        MonitorCommand.Params ret = new MonitorCommand.Params();
        ret.targetRegion = targetRegion;
        ret.pCtrlCliWithoutCluster = pCtrlCliWithoutCluster;
        ret.parentUrl = parentUrl;
        ret.sslFactory = sslFactory;
        ret.dateTime = dateTime;
        return ret;
      }

      public MonitorCommand.Params.Builder setTargetRegion(String targetRegion) {
        this.targetRegion = targetRegion;
        return this;
      }

      public MonitorCommand.Params.Builder setPCtrlCliWithoutCluster(ControllerClient pCtrlCliWithoutCluster) {
        this.pCtrlCliWithoutCluster = pCtrlCliWithoutCluster;
        return this;
      }

      public MonitorCommand.Params.Builder setParentUrl(String parentUrl) {
        this.parentUrl = parentUrl;
        return this;
      }

      public MonitorCommand.Params.Builder setSSLFactory(Optional<SSLFactory> sslFactory) {
        this.sslFactory = sslFactory;
        return this;
      }

      public MonitorCommand.Params.Builder setDateTime(LocalDateTime dateTime) {
        this.dateTime = dateTime;
        return this;
      }

    }
  }

  public static class Result extends Command.Result {
    private int futureVersion = Store.NON_EXISTING_VERSION;
    private String kafKaTopic;
    private StoreInfo storeInfo;

    public void setFutureVersion(int futureVersion) {
      this.futureVersion = futureVersion;
    }

    public void setKafKaTopic(String kafKaTopic) {
      this.kafKaTopic = kafKaTopic;
    }

    public int getFutureVersion() {
      return futureVersion;
    }

    public String getKafKaTopic() {
      return kafKaTopic;
    }

    public void setStoreInfo(StoreInfo storeInfo) {
      this.storeInfo = storeInfo;
    }
  }
}
