package com.linkedin.venice.controller.kafka.protocol.enums;

import com.linkedin.venice.controller.kafka.protocol.admin.AbortMigration;
import com.linkedin.venice.controller.kafka.protocol.admin.AddVersion;
import com.linkedin.venice.controller.kafka.protocol.admin.AdminOperation;
import com.linkedin.venice.controller.kafka.protocol.admin.ConfigureActiveActiveReplicationForCluster;
import com.linkedin.venice.controller.kafka.protocol.admin.ConfigureNativeReplicationForCluster;
import com.linkedin.venice.controller.kafka.protocol.admin.CreateStoragePersona;
import com.linkedin.venice.controller.kafka.protocol.admin.DeleteAllVersions;
import com.linkedin.venice.controller.kafka.protocol.admin.DeleteOldVersion;
import com.linkedin.venice.controller.kafka.protocol.admin.DeleteStoragePersona;
import com.linkedin.venice.controller.kafka.protocol.admin.DeleteStore;
import com.linkedin.venice.controller.kafka.protocol.admin.DerivedSchemaCreation;
import com.linkedin.venice.controller.kafka.protocol.admin.DisableStoreRead;
import com.linkedin.venice.controller.kafka.protocol.admin.EnableStoreRead;
import com.linkedin.venice.controller.kafka.protocol.admin.KillOfflinePushJob;
import com.linkedin.venice.controller.kafka.protocol.admin.MetaSystemStoreAutoCreationValidation;
import com.linkedin.venice.controller.kafka.protocol.admin.MetadataSchemaCreation;
import com.linkedin.venice.controller.kafka.protocol.admin.MigrateStore;
import com.linkedin.venice.controller.kafka.protocol.admin.PauseStore;
import com.linkedin.venice.controller.kafka.protocol.admin.PushStatusSystemStoreAutoCreationValidation;
import com.linkedin.venice.controller.kafka.protocol.admin.ResumeStore;
import com.linkedin.venice.controller.kafka.protocol.admin.SetStoreCurrentVersion;
import com.linkedin.venice.controller.kafka.protocol.admin.SetStoreOwner;
import com.linkedin.venice.controller.kafka.protocol.admin.SetStorePartitionCount;
import com.linkedin.venice.controller.kafka.protocol.admin.StoreCreation;
import com.linkedin.venice.controller.kafka.protocol.admin.SupersetSchemaCreation;
import com.linkedin.venice.controller.kafka.protocol.admin.UpdateStoragePersona;
import com.linkedin.venice.controller.kafka.protocol.admin.UpdateStore;
import com.linkedin.venice.controller.kafka.protocol.admin.ValueSchemaCreation;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceMessageException;
import java.util.HashMap;
import java.util.Map;


public enum AdminMessageType {
  STORE_CREATION(0, false), VALUE_SCHEMA_CREATION(1, false), DISABLE_STORE_WRITE(2, false),
  ENABLE_STORE_WRITE(3, false), KILL_OFFLINE_PUSH_JOB(4, false), DISABLE_STORE_READ(5, false),
  ENABLE_STORE_READ(6, false), DELETE_ALL_VERSIONS(7, false), SET_STORE_OWNER(8, false), SET_STORE_PARTITION(9, false),
  SET_STORE_CURRENT_VERSION(10, false), UPDATE_STORE(11, false), DELETE_STORE(12, false), DELETE_OLD_VERSION(13, false),
  MIGRATE_STORE(14, false), ABORT_MIGRATION(15, false), ADD_VERSION(16, false), DERIVED_SCHEMA_CREATION(17, false),
  SUPERSET_SCHEMA_CREATION(18, false), CONFIGURE_NATIVE_REPLICATION_FOR_CLUSTER(19, true),
  REPLICATION_METADATA_SCHEMA_CREATION(20, false), CONFIGURE_ACTIVE_ACTIVE_REPLICATION_FOR_CLUSTER(21, true),
  /**
   * @deprecated We do not support incremental push policy anymore.
   */
  @Deprecated
  CONFIGURE_INCREMENTAL_PUSH_FOR_CLUSTER(22, true), META_SYSTEM_STORE_AUTO_CREATION_VALIDATION(23, false),
  PUSH_STATUS_SYSTEM_STORE_AUTO_CREATION_VALIDATION(24, false), CREATE_STORAGE_PERSONA(25, false),
  DELETE_STORAGE_PERSONA(26, false), UPDATE_STORAGE_PERSONA(27, false);

  private final int value;
  private final boolean batchUpdate;
  private static final Map<Integer, AdminMessageType> MESSAGE_TYPE_MAP = getMessageTypeMap();

  AdminMessageType(int value, boolean batchUpdate) {
    this.value = value;
    this.batchUpdate = batchUpdate;
  }

  public Object getNewInstance() {
    switch (valueOf(value)) {
      case STORE_CREATION:
        return new StoreCreation();
      case VALUE_SCHEMA_CREATION:
        return new ValueSchemaCreation();
      case DISABLE_STORE_WRITE:
        return new PauseStore();
      case ENABLE_STORE_WRITE:
        return new ResumeStore();
      case KILL_OFFLINE_PUSH_JOB:
        return new KillOfflinePushJob();
      case DISABLE_STORE_READ:
        return new DisableStoreRead();
      case ENABLE_STORE_READ:
        return new EnableStoreRead();
      case DELETE_ALL_VERSIONS:
        return new DeleteAllVersions();
      case SET_STORE_OWNER:
        return new SetStoreOwner();
      case SET_STORE_PARTITION:
        return new SetStorePartitionCount();
      case SET_STORE_CURRENT_VERSION:
        return new SetStoreCurrentVersion();
      case UPDATE_STORE:
        return new UpdateStore();
      case DELETE_STORE:
        return new DeleteStore();
      case DELETE_OLD_VERSION:
        return new DeleteOldVersion();
      case MIGRATE_STORE:
        return new MigrateStore();
      case ABORT_MIGRATION:
        return new AbortMigration();
      case ADD_VERSION:
        return new AddVersion();
      case DERIVED_SCHEMA_CREATION:
        return new DerivedSchemaCreation();
      case SUPERSET_SCHEMA_CREATION:
        return new SupersetSchemaCreation();
      case CONFIGURE_NATIVE_REPLICATION_FOR_CLUSTER:
        return new ConfigureNativeReplicationForCluster();
      case REPLICATION_METADATA_SCHEMA_CREATION:
        return new MetadataSchemaCreation();
      case CONFIGURE_ACTIVE_ACTIVE_REPLICATION_FOR_CLUSTER:
        return new ConfigureActiveActiveReplicationForCluster();
      case META_SYSTEM_STORE_AUTO_CREATION_VALIDATION:
        return new MetaSystemStoreAutoCreationValidation();
      case PUSH_STATUS_SYSTEM_STORE_AUTO_CREATION_VALIDATION:
        return new PushStatusSystemStoreAutoCreationValidation();
      case CREATE_STORAGE_PERSONA:
        return new CreateStoragePersona();
      case DELETE_STORAGE_PERSONA:
        return new DeleteStoragePersona();
      case UPDATE_STORAGE_PERSONA:
        return new UpdateStoragePersona();
      default:
        throw new VeniceException("Unsupported " + getClass().getSimpleName() + " value: " + value);
    }
  }

  private static Map<Integer, AdminMessageType> getMessageTypeMap() {
    Map<Integer, AdminMessageType> intToTypeMap = new HashMap<>();
    for (AdminMessageType type: AdminMessageType.values()) {
      intToTypeMap.put(type.value, type);
    }
    return intToTypeMap;
  }

  private static AdminMessageType valueOf(int value) {
    AdminMessageType type = MESSAGE_TYPE_MAP.get(value);
    if (type == null) {
      throw new VeniceMessageException("Invalid admin message type: " + value);
    }
    return type;
  }

  public static AdminMessageType valueOf(AdminOperation adminMessage) {
    return valueOf(adminMessage.operationType);
  }

  public int getValue() {
    return value;
  }

  public boolean isBatchUpdate() {
    return batchUpdate;
  }
}
