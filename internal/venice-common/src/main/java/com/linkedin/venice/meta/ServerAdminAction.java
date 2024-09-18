package com.linkedin.venice.meta;

import java.util.HashMap;
import java.util.Map;


public enum ServerAdminAction {
  DUMP_INGESTION_STATE(0), DUMP_SERVER_CONFIGS(1);

  private static final Map<Integer, ServerAdminAction> ADMIN_ACTION_MAP = new HashMap<>(2);

  static {
    for (ServerAdminAction action: values()) {
      ADMIN_ACTION_MAP.put(action.getValue(), action);
    }
  }
  private final int value;

  ServerAdminAction(int value) {
    this.value = value;
  }

  public int getValue() {
    return this.value;
  }

  public static ServerAdminAction fromValue(int value) {
    ServerAdminAction action = ADMIN_ACTION_MAP.get(value);
    if (action == null) {
      throw new IllegalArgumentException("Unknown server admin action value: " + value);
    }
    return action;
  }
}
