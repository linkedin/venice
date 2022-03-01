package com.linkedin.venice.controllerapi;

import com.linkedin.venice.meta.RegionPushDetails;
import com.linkedin.venice.meta.StoreInfo;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


public class StoreHealthAuditResponse extends ControllerResponse {
  private String storeName = null;
  private String clusterName = null;
  private ArrayList<String> regionsWithStaleData = new ArrayList<>();
  private Map<String, Integer> regionToCurrentVersion = new HashMap<>();
  private Map<String, RegionPushDetails> regionPushDetails = new HashMap<>();
  private Map<Integer, ArrayList<String>> versionToRegion = new HashMap<>();

  public StoreHealthAuditResponse() { }

  public StoreHealthAuditResponse(String storeName) {
    this.storeName = storeName;
  }

  public StoreHealthAuditResponse(StoreInfo storeInfo) {
    setStoreName(storeInfo.getName());
  }

  public String getStoreName() {
    return storeName;
  }

  public void setStoreName(String storeName) {
    this.storeName = storeName;
  }

  public String getClusterName() {
    return clusterName;
  }

  public void setClusterName(String clusterName) {
    this.clusterName = clusterName;
  }

  public ArrayList<String> getRegionsWithStaleData() {
    return regionsWithStaleData;
  }

  public void setRegionsWithStaleData(ArrayList<String> regionsWithStaleData) {
    this.regionsWithStaleData = regionsWithStaleData;
  }

  public Map<String, Integer> getRegionToCurrentVersionMap() {
    return regionToCurrentVersion;
  }

  public void setRegionToCurrentVersion(Map<String, Integer> regionToCurrentVersion) {
    this.regionToCurrentVersion = regionToCurrentVersion;
  }

  public Map<String, RegionPushDetails> getRegionPushDetails() {
    return regionPushDetails;
  }

  public void setRegionPushDetails(Map<String, RegionPushDetails> newRegionPushDetails) {
    this.regionPushDetails = newRegionPushDetails;
    Integer highestVersion = 0;
    for (Map.Entry<String, RegionPushDetails> details : newRegionPushDetails.entrySet()) {
      String regionName = details.getKey();
      RegionPushDetails pushDetails = details.getValue();
      Integer currentVersion = pushDetails.getCurrentVersion();

      getRegionToCurrentVersionMap().put(regionName, currentVersion);
      if (!getVersionToRegionMap().containsKey(currentVersion))
        getVersionToRegionMap().put(currentVersion, new ArrayList<String>());
      getVersionToRegionMap().get(currentVersion).add(regionName);
      highestVersion = highestVersion < currentVersion ? currentVersion : highestVersion;
    }
    for (Map.Entry<String, RegionPushDetails> details : newRegionPushDetails.entrySet())
      if (details.getValue().getCurrentVersion() < highestVersion)
        regionsWithStaleData.add(details.getKey());
  }

  public Map<Integer, ArrayList<String>> getVersionToRegionMap() {
    return versionToRegion;
  }

  public void setVersionToRegion(Map<Integer, ArrayList<String>> versionToRegion) {
    this.versionToRegion.clear();
    for (Map.Entry<Integer, ArrayList<String>> entry : versionToRegion.entrySet())
      this.versionToRegion.put(entry.getKey(), (ArrayList<String>)entry.getValue().clone());
    this.versionToRegion = versionToRegion;
  }
}
