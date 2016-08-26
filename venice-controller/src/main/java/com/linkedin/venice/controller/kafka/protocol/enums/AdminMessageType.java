package com.linkedin.venice.controller.kafka.protocol.enums;

import com.linkedin.venice.controller.kafka.protocol.admin.AdminOperation;
import com.linkedin.venice.controller.kafka.protocol.admin.KeySchemaCreation;
import com.linkedin.venice.controller.kafka.protocol.admin.StoreCreation;
import com.linkedin.venice.controller.kafka.protocol.admin.ValueSchemaCreation;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceMessageException;

import java.util.HashMap;
import java.util.Map;

public enum AdminMessageType {
  STORE_CREATION(0),
  KEY_SCHEMA_CREATION(1),
  VALUE_SCHEMA_CREATION(2);

  private final int value;
  private static final Map<Integer, AdminMessageType> MESSAGE_TYPE_MAP = getMessageTypeMap();

  AdminMessageType(int value) {
    this.value = value;
  }

  public Object getNewInstance() {
    switch (valueOf(value)) {
      case STORE_CREATION: return new StoreCreation();
      case KEY_SCHEMA_CREATION: return new KeySchemaCreation();
      case VALUE_SCHEMA_CREATION: return new ValueSchemaCreation();
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
}
