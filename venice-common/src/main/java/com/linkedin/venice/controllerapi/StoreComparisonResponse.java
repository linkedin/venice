package com.linkedin.venice.controllerapi;

import com.linkedin.venice.meta.VersionStatus;
import java.util.HashMap;
import java.util.Map;


public class StoreComparisonResponse extends ControllerResponse {
  Map<String, Map<String, String>> propertyDiff = new HashMap<>();
  Map<String, Map<String, String>> schemaDiff = new HashMap<>();
  Map<String, Map<Integer, VersionStatus>> versionStateDiff = new HashMap<>();

  public Map<String, Map<String, String>> getPropertyDiff() {
    return propertyDiff;
  }

  public void setPropertyDiff(Map<String, Map<String, String>> propertyDiff) {
    this.propertyDiff = propertyDiff;
  }

  public Map<String, Map<String, String>> getSchemaDiff() {
    return schemaDiff;
  }

  public void setSchemaDiff(Map<String, Map<String, String>> schemaDiff) {
    this.schemaDiff = schemaDiff;
  }

  public Map<String, Map<Integer, VersionStatus>> getVersionStateDiff() {
    return versionStateDiff;
  }

  public void setVersionStateDiff(Map<String, Map<Integer, VersionStatus>> versionStateDiff) {
    this.versionStateDiff = versionStateDiff;
  }
}
