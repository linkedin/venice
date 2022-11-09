package com.linkedin.davinci.ingestion.main;

public enum MainPartitionIngestionStatus {
  MAIN, // Partition is being ingested in main process
  ISOLATED, // Partition is being ingested in isolated process
  NOT_EXIST // Partition is not being ingested in the host
}
