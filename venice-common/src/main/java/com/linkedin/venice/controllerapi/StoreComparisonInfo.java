package com.linkedin.venice.controllerapi;

import com.linkedin.venice.meta.VersionStatus;
import java.util.HashMap;
import java.util.Map;


public class StoreComparisonInfo {
  Map<String, Map<String, String>> propertyDiff = new HashMap<>();
  Map<String, Map<String, String>> schemaDiff = new HashMap<>();
  Map<String, Map<Integer, VersionStatus>> versionStateDiff = new HashMap<>();

  public Map<String, Map<String, String>> getPropertyDiff() {
    return propertyDiff;
  }

  public void addPropertyDiff(
      String fabricA,
      String fabricB,
      String propertyName,
      String propertyValueA,
      String propertyValueB) {
    propertyDiff.computeIfAbsent(fabricA, k -> new HashMap<>());
    propertyDiff.computeIfAbsent(fabricB, k -> new HashMap<>());
    propertyDiff.get(fabricA).put(propertyName, propertyValueA);
    propertyDiff.get(fabricB).put(propertyName, propertyValueB);
  }

  public Map<String, Map<String, String>> getSchemaDiff() {
    return schemaDiff;
  }

  public void addSchemaDiff(String fabricA, String fabricB, String schemaName, String schemaA, String schemaB) {
    schemaDiff.computeIfAbsent(fabricA, k -> new HashMap<>());
    schemaDiff.computeIfAbsent(fabricB, k -> new HashMap<>());
    schemaDiff.get(fabricA).put(schemaName, schemaA);
    schemaDiff.get(fabricB).put(schemaName, schemaB);
  }

  public Map<String, Map<Integer, VersionStatus>> getVersionStateDiff() {
    return versionStateDiff;
  }

  public void addVersionStateDiff(
      String fabricA,
      String fabricB,
      int versionNum,
      VersionStatus versionStatusA,
      VersionStatus versionStatusB) {
    versionStateDiff.computeIfAbsent(fabricA, k -> new HashMap<>());
    versionStateDiff.computeIfAbsent(fabricB, k -> new HashMap<>());
    versionStateDiff.get(fabricA).put(versionNum, versionStatusA);
    versionStateDiff.get(fabricB).put(versionNum, versionStatusB);
  }
}
