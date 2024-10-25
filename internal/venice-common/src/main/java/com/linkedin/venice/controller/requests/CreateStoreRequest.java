package com.linkedin.venice.controller.requests;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.venice.controllerapi.ControllerRoute;


public class CreateStoreRequest extends ControllerRequest {
  private final String storeName;
  private final String owner;

  private final String keySchema;

  private final String valueSchema;

  private final boolean systemStore;

  private final String accessPermissions;

  @VisibleForTesting
  CreateStoreRequest(
      String clusterName,
      String storeName,
      String owner,
      String keySchema,
      String valueSchema,
      String accessPermissions,
      boolean systemStore) {
    super(clusterName, ControllerRoute.NEW_STORE);
    this.storeName = storeName;
    this.owner = owner;
    this.keySchema = keySchema;
    this.valueSchema = valueSchema;
    this.accessPermissions = accessPermissions;
    this.systemStore = systemStore;
  }

  public String getStoreName() {
    return storeName;
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
    return systemStore;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder extends ControllerRequest.Builder<Builder> {
    private String storeName;
    private String owner;
    private String keySchema;
    private String valueSchema;

    private String accessPermissions;
    private boolean systemStore;

    public Builder setStoreName(String storeName) {
      this.storeName = storeName;
      return getThis();
    }

    public Builder setOwner(String owner) {
      this.owner = owner;
      return getThis();
    }

    public Builder setKeySchema(String keySchema) {
      this.keySchema = keySchema;
      return getThis();
    }

    public Builder setValueSchema(String valueSchema) {
      this.valueSchema = valueSchema;
      return getThis();
    }

    public Builder setAccessPermissions(String accessPermissions) {
      this.accessPermissions = accessPermissions;
      return getThis();
    }

    public Builder setSystemStore(boolean isSystemStore) {
      this.systemStore = isSystemStore;
      return getThis();
    }

    public CreateStoreRequest build() {
      return new CreateStoreRequest(
          clusterName,
          storeName,
          owner,
          keySchema,
          valueSchema,
          accessPermissions,
          systemStore);
    }

    @Override
    Builder getThis() {
      return this;
    }
  }
}
