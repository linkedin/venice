package com.linkedin.davinci.ingestion.isolated;

/**
 * This enum class denotes a resource ingestion status in isolated process.
 */
public enum IsolatedIngestionStatus {
  UNSUBSCRIBED, // The resource has been handed over to main process.
  RUNNING, // The resource is ingesting
  STOPPED // The resource has stopped ingesting
}
