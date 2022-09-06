package com.linkedin.venice.meta;

public enum ServerAdminAction {
  DUMP_INGESTION_STATE(0), DUMP_SERVER_CONFIGS(1);

  private final int value;

  ServerAdminAction(int value) {
    this.value = value;
  }

  public int getValue() {
    return this.value;
  }
}
