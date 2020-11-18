package com.linkedin.venice.meta;

public enum IngestionAction {
  INIT,
  COMMAND,
  REPORT,
  METRIC,
  UPDATE_METADATA,
  SHUTDOWN_COMPONENT
}
