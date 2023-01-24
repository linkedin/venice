package com.linkedin.venice.meta;

import com.linkedin.venice.pushmonitor.OfflinePushStatus;
import com.linkedin.venice.pushmonitor.PartitionStatus;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;


public class RegionPushDetails {
  private String regionName = null;
  private String pushStartLocalDateTime = null;
  private String pushEndLocalDateTime = null;
  private String dataAge = null;
  private String latestFailedPush = null;
  private String errorMessage = null;
  private ArrayList<Integer> versions = new ArrayList<>();
  private Integer currentVersion = -1;
  private List<PartitionDetail> partitionDetails = new ArrayList<>();

  public List<PartitionDetail> getPartitionDetails() {
    return partitionDetails;
  }

  public void setPartitionDetails(List<PartitionDetail> partitionDetails) {
    this.partitionDetails = partitionDetails;
  }

  public ArrayList<Integer> getVersions() {
    return versions;
  }

  public void setVersions(ArrayList<Integer> versions) {
    this.versions = versions;
  }

  public Integer getCurrentVersion() {
    return currentVersion;
  }

  public void setCurrentVersion(Integer currentVersion) {
    this.currentVersion = currentVersion;
  }

  public RegionPushDetails(String regionName) {
    this.regionName = regionName;
  }

  public RegionPushDetails() {
  }

  public String getRegionName() {
    return regionName;
  }

  public void setRegionName(String regionName) {
    this.regionName = regionName;
  }

  public String getPushStartTimestamp() {
    return pushStartLocalDateTime;
  }

  public void setPushStartTimestamp(String pushStartLocalDateTime) {
    this.pushStartLocalDateTime = pushStartLocalDateTime;
    updateDataAge();
  }

  public String getPushEndTimestamp() {
    return pushEndLocalDateTime;
  }

  public void setPushEndTimestamp(String pushEndLocalDateTime) {
    this.pushEndLocalDateTime = pushEndLocalDateTime;
    updateDataAge();
  }

  public void updateDataAge() {
    if (getPushStartTimestamp() != null && getPushEndTimestamp() != null) {
      LocalDateTime start = LocalDateTime.parse(getPushStartTimestamp());
      LocalDateTime end = LocalDateTime.parse(getPushEndTimestamp());
      Duration age = Duration.between(end, start);
      setDataAge(age.toString());
    }
  }

  public String getDataAge() {
    return dataAge;
  }

  public void setDataAge(String dataAge) {
    this.dataAge = dataAge;
  }

  public String getLatestFailedPush() {
    return latestFailedPush;
  }

  public void setLatestFailedPush(String latestFailedPush) {
    this.latestFailedPush = latestFailedPush;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public void setErrorMessage(String errorMessage) {
    this.errorMessage = errorMessage;
  }

  public void addVersion(int version) {
    versions.add(version);
  }

  public void addPartitionDetails(final OfflinePushStatus status) {
    int numOfPartition = status.getNumberOfPartition();
    for (int pid = 0; pid < numOfPartition; pid++) {
      PartitionStatus partition = status.getPartitionStatus(pid);
      if (partition == null) {
        continue;
      }
      PartitionDetail pDetail = new PartitionDetail(partition.getPartitionId());
      pDetail.addReplicaDetails(partition);
      partitionDetails.add(pDetail);
    }
  }

  @Override
  public String toString() {
    return "RegionPushDetails{" + "regionName='" + regionName + '\'' + ", pushStartLocalDateTime='"
        + pushStartLocalDateTime + '\'' + ", pushEndLocalDateTime='" + pushEndLocalDateTime + '\'' + ", dataAge='"
        + dataAge + '\'' + ", latestFailedPush='" + latestFailedPush + '\'' + ", errorMessage='" + errorMessage + '\''
        + ", versions=" + versions + ", currentVersion=" + currentVersion + ", partitionDetails=" + partitionDetails
        + '}';
  }
}
