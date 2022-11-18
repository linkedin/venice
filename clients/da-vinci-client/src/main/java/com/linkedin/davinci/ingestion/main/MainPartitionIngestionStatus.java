package com.linkedin.davinci.ingestion.main;

public enum MainPartitionIngestionStatus {
  // Partition is being ingested in main process
  MAIN,
  // Partition is being ingested in isolated process
  ISOLATED,
  // Partition is not being ingested in the host
  NOT_EXIST
}
