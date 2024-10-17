package com.linkedin.venice.controllerapi.request;

/**
 * Represents a request to create a new store in the specified Venice cluster with the provided parameters.
 * This class encapsulates all necessary details for the creation of a store, including its name, owner,
 * schema definitions, and access permissions.
 */
public class CreateNewStoreRequest extends ControllerRequest {
  public static final String DEFAULT_STORE_OWNER = "";

  private final String owner;
  private final String keySchema;
  private final String valueSchema;
  private final boolean isSystemStore;

  // a JSON string representing the access permissions for the store
  private final String accessPermissions;

  public CreateNewStoreRequest(
      String clusterName,
      String storeName,
      String owner,
      String keySchema,
      String valueSchema,
      String accessPermissions,
      boolean isSystemStore) {
    super(clusterName, storeName);
    this.keySchema = validateParam(keySchema, "Key schema");
    this.valueSchema = validateParam(valueSchema, "Value schema");
    this.owner = owner == null ? DEFAULT_STORE_OWNER : owner;
    this.accessPermissions = accessPermissions;
    this.isSystemStore = isSystemStore;
  }

  public String getOwner() {
    return owner;
  }

  public String getKeySchema() {
    return keySchema;
  }

  public String getValueSchema() {
    return valueSchema;
  }

  public String getAccessPermissions() {
    return accessPermissions;
  }

  public boolean isSystemStore() {
    return isSystemStore;
  }
}
