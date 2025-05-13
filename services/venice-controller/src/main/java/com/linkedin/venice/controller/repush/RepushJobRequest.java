package com.linkedin.venice.controller.repush;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.CLUSTER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.SOURCE_REGION;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.STORE_NAME;

import com.linkedin.venice.stats.dimensions.RepushStoreTriggerSource;
import java.util.HashMap;
import java.util.Map;


public class RepushJobRequest {
  public static final RepushStoreTriggerSource SCHEDULED_TRIGGER = RepushStoreTriggerSource.SCHEDULED;
  public static final RepushStoreTriggerSource MANUAL_TRIGGER = RepushStoreTriggerSource.MANUAL;

  private final String clusterName;
  private final String storeName;
  private final String sourceRegion;
  private final RepushStoreTriggerSource triggerSource;

  public RepushJobRequest(String clusterName, String storeName, RepushStoreTriggerSource triggerSource) {
    this.clusterName = clusterName;
    this.storeName = storeName;
    this.sourceRegion = null; // default to null if not specified
    this.triggerSource = triggerSource;
  }

  public RepushJobRequest(
      String clusterName,
      String storeName,
      String sourceRegion,
      RepushStoreTriggerSource triggerSource) {
    this.clusterName = clusterName;
    this.storeName = storeName;
    this.sourceRegion = sourceRegion;
    this.triggerSource = triggerSource;
  }

  public String getClusterName() {
    return clusterName;
  }

  public String getStoreName() {
    return storeName;
  }

  public String getSourceRegion() {
    return sourceRegion;
  }

  public RepushStoreTriggerSource getTriggerSource() {
    return triggerSource;
  }

  public Map<String, Object> toParams() {
    Map<String, Object> params = new HashMap<>();
    params.put(CLUSTER, clusterName);
    params.put(STORE_NAME, storeName);
    params.put(SOURCE_REGION, sourceRegion);
    return params;
  }

  @Override
  public String toString() {
    return "RepushJobRequest {" + "clusterName='" + this.clusterName + '\'' + "storeName='" + this.storeName + '\''
        + ", sourceRegion='" + this.sourceRegion + '\'' + ", triggerSource='" + this.triggerSource + '\'' + '}';
  }
}
