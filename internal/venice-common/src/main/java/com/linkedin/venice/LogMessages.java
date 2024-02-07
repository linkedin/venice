package com.linkedin.venice;

public class LogMessages {
  // Replica with ERROR status with the following message implies the job was killed instead of actual ingestion error
  public static final String KILLED_JOB_MESSAGE = "Received the signal to kill this consumer. Topic ";

}
