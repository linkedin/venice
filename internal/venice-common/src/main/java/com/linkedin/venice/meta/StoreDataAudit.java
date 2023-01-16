package com.linkedin.venice.meta;

import java.util.HashMap;
import java.util.Map;


/**
 * This is a class used to manage multiple replicas of the same store.
 * The insert function will determine the latest version of the store
 * across all replicas, and store the StoreInfo in either healthy or
 * stale region map, rebalancing these when needed. The class also
 * keeps version info about replicas at the top level, with latestCreatedVersion
 * and latestSuccessfulPushVersion being maintained internally.
 */

public class StoreDataAudit {
  private Map<String, StoreInfo> healthyRegions;
  private Map<String, StoreInfo> staleRegions;
  private int latestCreatedVersion = 0;
  private int latestSuccessfulPushVersion = 0;
  private String storeName = "";

  public StoreDataAudit() {
    healthyRegions = new HashMap<String, StoreInfo>();
    staleRegions = new HashMap<String, StoreInfo>();
  }

  public Map<String, StoreInfo> getHealthyRegions() {
    return healthyRegions;
  }

  public Map<String, StoreInfo> getStaleRegions() {
    return staleRegions;
  }

  public int getLatestCreatedVersion() {
    return latestCreatedVersion;
  }

  public int getLatestSuccessfulPushVersion() {
    return latestSuccessfulPushVersion;
  }

  public String getStoreName() {
    return storeName;
  }

  public void setHealthyRegions(Map<String, StoreInfo> healthyRegions) {
    this.healthyRegions = healthyRegions;
  }

  public void setStaleRegions(Map<String, StoreInfo> staleRegions) {
    this.staleRegions = staleRegions;
  }

  public void setLatestCreatedVersion(int latestCreatedVersion) {
    this.latestCreatedVersion = latestCreatedVersion;
  }

  public void setLatestSuccessfulPushVersion(int latestSuccessfulPushVersion) {
    this.latestSuccessfulPushVersion = latestSuccessfulPushVersion;
  }

  public void setStoreName(String storeName) {
    this.storeName = storeName;
  }

  public void insert(String regionName, StoreInfo info) {
    if (storeName.equals(""))
      storeName = info.getName();
    if (getLatestCreatedVersion() == 0) {
      setLatestCreatedVersion(info.getLargestUsedVersionNumber());
    }
    if (getLatestSuccessfulPushVersion() == 0) {
      setLatestSuccessfulPushVersion(info.getCurrentVersion());
    }
    if (getLatestCreatedVersion() != info.getCurrentVersion()) {
      if (getLatestCreatedVersion() > info.getLargestUsedVersionNumber()) { // new store is stale
        staleRegions.put(regionName, info);
      } else { // new store is latest, make all healthy regions stale, add new info to healthy region
        healthyRegions.forEach((key, value) -> staleRegions.merge(key, value, (a, b) -> value));
        healthyRegions.clear();
        healthyRegions.put(regionName, info);
      }
      setLatestCreatedVersion(info.getLargestUsedVersionNumber());
    } else { // new store is equal to latest, add info to healthy region
      healthyRegions.put(regionName, info);
    }
  }

  public void merge(StoreDataAudit a) {
    a.getHealthyRegions().forEach((region, info) -> {
      this.insert(region, info);
    });
  }

  public StoreInfo getAny() {
    if (healthyRegions.size() > 0) {
      return healthyRegions.entrySet().iterator().next().getValue();
    } else if (staleRegions.size() > 0) {
      return staleRegions.entrySet().iterator().next().getValue();
    } else {
      return null;
    }
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{ \"");
    sb.append(getStoreName());
    sb.append("\": { healthy: {");
    for (Map.Entry<String, StoreInfo> entry: getHealthyRegions().entrySet()) {
      sb.append("\"");
      sb.append(entry.getKey());
      sb.append("\", ");
    }
    sb.append(" }, stale: { ");
    for (Map.Entry<String, StoreInfo> entry: getStaleRegions().entrySet()) {
      sb.append("\"");
      sb.append(entry.getKey());
      sb.append("\", ");
    }
    sb.append(" } }");
    return sb.toString();
  }
}
