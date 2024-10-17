package com.linkedin.venice.controller.server;

public class CreateStoreRequest {
  String clusterName;
  String storeName;
  String keySchema;
  String valueSchema;
  boolean isSystemStore;
  String accessPerm;

  public CreateStoreRequest(
      String clusterName,
      String storeName,
      String keySchema,
      String valueSchema,
      boolean isSystemStore,
      String accessPerm) {
    this.clusterName = clusterName;
    this.storeName = storeName;
    this.keySchema = keySchema;
    this.valueSchema = valueSchema;
    this.isSystemStore = isSystemStore;
    this.accessPerm = accessPerm;
  }

  public CreateStoreRequest() {
  }

  public String getClusterName() {
    return clusterName;
  }

  public void setClusterName(String clusterName) {
    this.clusterName = clusterName;
  }

  public String getStoreName() {
    return storeName;
  }

  public void setStoreName(String storeName) {
    this.storeName = storeName;
  }

  public String getKeySchema() {
    return keySchema;
  }

  public void setKeySchema(String keySchema) {
    this.keySchema = keySchema;
  }

  public String getValueSchema() {
    return valueSchema;
  }

  public void setValueSchema(String valueSchema) {
    this.valueSchema = valueSchema;
  }

  public boolean isSystemStore() {
    return isSystemStore;
  }

  public void setSystemStore(boolean isSystemStore) {
    this.isSystemStore = isSystemStore;
  }

  public String getAccessPerm() {
    return accessPerm;
  }

  public void setAccessPerm(String accessPerm) {
    this.accessPerm = accessPerm;
  }
}
