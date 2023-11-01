package com.linkedin.venice.hadoop.task;

/**
 * An interface to report task health and track task progress for different execution engines
 */
public interface TaskTracker {
  default void heartbeat() {
  }

  default float getProgress() {
    return -1;
  }
}
