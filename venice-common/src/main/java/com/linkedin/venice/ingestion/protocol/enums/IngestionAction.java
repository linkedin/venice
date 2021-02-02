package com.linkedin.venice.ingestion.protocol.enums;

public enum IngestionAction {
  INIT,
  COMMAND,
  REPORT,
  METRIC,
  HEARTBEAT,
  UPDATE_METADATA,
  SHUTDOWN_COMPONENT
}
