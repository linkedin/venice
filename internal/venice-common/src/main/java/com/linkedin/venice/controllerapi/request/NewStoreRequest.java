package com.linkedin.venice.controllerapi.request;

import java.util.Objects;


/**
 * Represents a request to create a new store in the specified Venice cluster with the provided parameters.
 * This class encapsulates all necessary details for the creation of a store, including its name, owner,
 * schema definitions, and access permissions.
 */
public class NewStoreRequest extends ControllerRequest {
  private final String storeName;
  private String owner;
  private String keySchema;
  private String valueSchema;
  private boolean isSystemStore;

  // a JSON string representing the access permissions for the store
  private String accessPermissions;

  public NewStoreRequest(
      String clusterName,
      String storeName,
      String owner,
      String keySchema,
      String valueSchema,
      String accessPermissions,
      boolean isSystemStore) {
    super(clusterName);
    this.storeName = Objects.requireNonNull(storeName, "Store name cannot be null when creating a new store");
    this.owner = owner;
    this.keySchema = keySchema;
    this.valueSchema = valueSchema;
    this.accessPermissions = accessPermissions;
    this.isSystemStore = isSystemStore;
  }

  public String getStoreName() {
    return storeName;
  }

  public String getOwner() {
    return owner;
  }

  public void setOwner(String owner) {
    this.owner = owner;
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

  public String getAccessPermissions() {
    return accessPermissions;
  }

  public void setAccessPermissions(String accessPermissions) {
    this.accessPermissions = accessPermissions;
  }

  public boolean isSystemStore() {
    return isSystemStore;
  }

  public void setSystemStore(boolean isSystemStore) {
    this.isSystemStore = isSystemStore;
  }
}
