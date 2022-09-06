package com.linkedin.venice.meta;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;


public class RegionPushDetails {
  private String regionName = null;
  private String pushStartLocalDateTime = null;
  private String pushEndLocalDateTime = null;
  private String dataAge = null;
  private String latestFailedPush = null;
  private String errorMessage = null;
  private ArrayList<Integer> versions = new ArrayList<>();
  private Integer currentVersion = -1;

  public Integer getCurrentVersion() {
    return currentVersion;
  }

  public void setCurrentVersion(Integer currentVersion) {
    this.currentVersion = currentVersion;
  }

  public RegionPushDetails(String _regionName) {
    setRegionName(_regionName);
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

  public ArrayList<Integer> getVersions() {
    return versions;
  }

  public void setVersions(ArrayList<Integer> versions) {
    this.versions = (ArrayList<Integer>) versions.clone();
  }

}
