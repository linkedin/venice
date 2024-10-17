package com.linkedin.venice.controller.server.endpoints;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.*;

import com.linkedin.venice.utils.Utils;


public class JobStatusRequest {
  private String cluster;
  private String store;
  private int versionNumber;
  private String incrementalPushVersion;
  private String targetedRegions;
  private String region;

  public JobStatusRequest() {
  }

  public void setCluster(String cluster) {
    this.cluster = cluster;
  }

  public String getCluster() {
    return cluster;
  }

  public void setStore(String store) {
    this.store = store;
  }

  public String getStore() {
    return store;
  }

  public void setVersionNumber(String versionNumber) {
    this.versionNumber = parseVersionNumber(versionNumber);
  }

  public void setVersionNumber(int versionNumber) {
    this.versionNumber = versionNumber;
  }

  private int parseVersionNumber(String versionNumber) {
    return Utils.parseIntFromString(versionNumber, VERSION);
  }

  public int getVersionNumber() {
    return versionNumber;
  }

  public void setIncrementalPushVersion(String incrementalPushVersion) {
    this.incrementalPushVersion = incrementalPushVersion;
  }

  public String getIncrementalPushVersion() {
    return incrementalPushVersion;
  }

  public void setTargetedRegions(String targetedRegions) {
    this.targetedRegions = targetedRegions;
  }

  public String getTargetedRegions() {
    return targetedRegions;
  }

  public void setRegion(String region) {
    this.region = region;
  }

  public String getRegion() {
    return region;
  }
}
