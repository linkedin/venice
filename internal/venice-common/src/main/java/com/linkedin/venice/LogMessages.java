package com.linkedin.venice;

public class LogMessages {
  /** Replica with ERROR status with the following message implies the job was killed instead of actual ingestion error
   * This is used in server-controller communication to detect killed failed pushes. and should not be modified
   */
  public static final String KILLED_JOB_MESSAGE = "Received the signal to kill this consumer. Topic ";

}
