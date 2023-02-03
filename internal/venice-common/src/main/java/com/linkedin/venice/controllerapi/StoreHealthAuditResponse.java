package com.linkedin.venice.controllerapi;

import com.linkedin.venice.meta.RegionPushDetails;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


public class StoreHealthAuditResponse extends ControllerResponse {
  private ArrayList<String> regionsWithStaleData = new ArrayList<>();
  private Map<String, Integer> regionToCurrentVersion = new HashMap<>();
  private Map<String, RegionPushDetails> regionPushDetails = new HashMap<>();
  private Map<Integer, ArrayList<String>> versionToRegion = new HashMap<>();

  public ArrayList<String> getRegionsWithStaleData() {
    return regionsWithStaleData;
  }

  public void setRegionsWithStaleData(ArrayList<String> regionsWithStaleData) {
    this.regionsWithStaleData = regionsWithStaleData;
  }

  public Map<String, RegionPushDetails> getRegionPushDetails() {
    return regionPushDetails;
  }

  public void setRegionPushDetails(Map<String, RegionPushDetails> newRegionPushDetails) {
    this.regionPushDetails = newRegionPushDetails;
    Integer highestVersion = 0;
    for (Map.Entry<String, RegionPushDetails> details: newRegionPushDetails.entrySet()) {
      String regionName = details.getKey();
      Integer currentVersion = details.getValue().getCurrentVersion();

      populateRegionToCurrentVersion(currentVersion, regionName);
      populateVersionToRegion(currentVersion, regionName);

      highestVersion = Math.max(currentVersion, highestVersion);
    }

    for (Map.Entry<String, RegionPushDetails> details: newRegionPushDetails.entrySet()) {
      if (details.getValue().getCurrentVersion() < highestVersion) {
        regionsWithStaleData.add(details.getKey());
      }
    }
  }

  private void populateRegionToCurrentVersion(int version, String regionName) {
    regionToCurrentVersion.put(regionName, version);
  }

  private void populateVersionToRegion(int version, String name) {
    versionToRegion.putIfAbsent(version, new ArrayList<>());
    versionToRegion.get(version).add(name);
  }

  @Override
  public String toString() {
    return "StoreHealthAuditResponse{" + "regionsWithStaleData=" + regionsWithStaleData + ", regionToCurrentVersion="
        + regionToCurrentVersion + ", regionPushDetails=" + regionPushDetails + ", versionToRegion=" + versionToRegion
        + '}';
  }
}
