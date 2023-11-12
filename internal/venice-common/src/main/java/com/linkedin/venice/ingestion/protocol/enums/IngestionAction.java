package com.linkedin.venice.ingestion.protocol.enums;

public enum IngestionAction {
  COMMAND, REPORT, METRIC, HEARTBEAT, UPDATE_METADATA, SHUTDOWN_COMPONENT, GET_LOADED_STORE_USER_PARTITION_MAPPING
}
