package com.linkedin.venice.controller.kafka.protocol.enums;

import com.linkedin.venice.controller.kafka.protocol.admin.AbortMigration;
import com.linkedin.venice.controller.kafka.protocol.admin.AdminOperation;
import com.linkedin.venice.controller.kafka.protocol.admin.DeleteAllVersions;
import com.linkedin.venice.controller.kafka.protocol.admin.DeleteOldVersion;
import com.linkedin.venice.controller.kafka.protocol.admin.DeleteStore;
import com.linkedin.venice.controller.kafka.protocol.admin.DisableStoreRead;
import com.linkedin.venice.controller.kafka.protocol.admin.EnableStoreRead;
import com.linkedin.venice.controller.kafka.protocol.admin.KillOfflinePushJob;
import com.linkedin.venice.controller.kafka.protocol.admin.MigrateStore;
import com.linkedin.venice.controller.kafka.protocol.admin.PauseStore;
import com.linkedin.venice.controller.kafka.protocol.admin.ResumeStore;
import com.linkedin.venice.controller.kafka.protocol.admin.SetStoreCurrentVersion;
import com.linkedin.venice.controller.kafka.protocol.admin.SetStoreOwner;
import com.linkedin.venice.controller.kafka.protocol.admin.SetStorePartitionCount;
import com.linkedin.venice.controller.kafka.protocol.admin.StoreCreation;
import com.linkedin.venice.controller.kafka.protocol.admin.UpdateStore;
import com.linkedin.venice.controller.kafka.protocol.admin.ValueSchemaCreation;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceMessageException;
import java.util.HashMap;
import java.util.Map;

public enum AdminMessageType {
  STORE_CREATION(0),
  VALUE_SCHEMA_CREATION(1),
  DISABLE_STORE_WRITE(2),
  ENABLE_STORE_WRITE(3),
  KILL_OFFLINE_PUSH_JOB(4),
  DISABLE_STORE_READ(5),
  ENABLE_STORE_READ(6),
  DELETE_ALL_VERSIONS(7),
  SET_STORE_OWNER(8),
  SET_STORE_PARTITION(9),
  SET_STORE_CURRENT_VERSION(10),
  UPDATE_STORE(11),
  DELETE_STORE(12),
  DELETE_OLD_VERSION(13),
  MIGRATE_STORE(14),
  ABORT_MIGRATION(15);

  private final int value;
  private static final Map<Integer, AdminMessageType> MESSAGE_TYPE_MAP = getMessageTypeMap();

  AdminMessageType(int value) {
    this.value = value;
  }

  public Object getNewInstance() {
    switch (valueOf(value)) {
      case STORE_CREATION: return new StoreCreation();
      case VALUE_SCHEMA_CREATION: return new ValueSchemaCreation();
      case DISABLE_STORE_WRITE: return new PauseStore();
      case ENABLE_STORE_WRITE: return new ResumeStore();
      case KILL_OFFLINE_PUSH_JOB: return new KillOfflinePushJob();
      case DISABLE_STORE_READ: return new DisableStoreRead();
      case ENABLE_STORE_READ: return new EnableStoreRead();
      case DELETE_ALL_VERSIONS: return new DeleteAllVersions();
      case SET_STORE_OWNER: return new SetStoreOwner();
      case SET_STORE_PARTITION: return new SetStorePartitionCount();
      case SET_STORE_CURRENT_VERSION: return new SetStoreCurrentVersion();
      case UPDATE_STORE: return new UpdateStore();
      case DELETE_STORE: return new DeleteStore();
      case DELETE_OLD_VERSION: return new DeleteOldVersion();
      case MIGRATE_STORE: return new MigrateStore();
      case ABORT_MIGRATION: return new AbortMigration();
      default: throw new VeniceException("Unsupported " + getClass().getSimpleName() + " value: " + value);
    }
  }

  private static Map<Integer, AdminMessageType> getMessageTypeMap() {
    Map<Integer, AdminMessageType> intToTypeMap = new HashMap<>();
    for (AdminMessageType type : AdminMessageType.values()) {
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
}
