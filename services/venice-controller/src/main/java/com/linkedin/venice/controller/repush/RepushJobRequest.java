package com.linkedin.venice.controller.repush;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.SOURCE_REGION;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.STORE_NAME;

import java.util.HashMap;
import java.util.Map;


public class RepushJobRequest {
  public static final String SCHEDULED_TRIGGER = "Scheduled";
  public static final String MANUAL_TRIGGER = "Manual";

  private final String storeName;
  private final String sourceRegion;
  private final String triggerSource;

  public RepushJobRequest(String storeName, String triggerSource) {
    this.storeName = storeName;
    this.sourceRegion = null; // default to null if not specified
    this.triggerSource = triggerSource;
  }

  public RepushJobRequest(String storeName, String sourceRegion, String triggerSource) {
    this.storeName = storeName;
    this.sourceRegion = sourceRegion;
    this.triggerSource = triggerSource;
  }

  public String getStoreName() {
    return storeName;
  }

  public String getSourceRegion() {
    return sourceRegion;
  }

  public String getTriggerSource() {
    return triggerSource;
  }

  public Map<String, Object> toParams() {
    Map<String, Object> params = new HashMap<>();
    params.put(STORE_NAME, storeName);
    params.put(SOURCE_REGION, sourceRegion);
    return params;
  }
}
