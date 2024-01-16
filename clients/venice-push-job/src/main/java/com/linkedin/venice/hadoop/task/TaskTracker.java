package com.linkedin.venice.hadoop.task;

/**
 * An interface to report task health and track task progress for different execution engines
 */
public interface TaskTracker {
  float PROGRESS_NOT_SUPPORTED = -1.0f;
  float PROGRESS_COMPLETED = 1.0f;

  /**
   * Report task heartbeat if the execution engine requires it
   */
  default void heartbeat() {
  }

  /**
   * Get the progress of the task.
   * If the engine supports fetching the progress, progress is represented as a number between 0 and 1 (inclusive).
   * If the engine doesn't support fetching the progress, a value of -1 is returned.
   */
  default float getProgress() {
    return PROGRESS_NOT_SUPPORTED;
  }
}
