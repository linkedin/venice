package com.linkedin.venice.hadoop.engine;

import java.util.Properties;


/**
 * An interface to get information about a running job and task on any batch computing engine.
 */
public interface EngineTaskConfigProvider {
  /**
   * @return The name of the compute job. This must be the same for all tasks of the same job.
   */
  String getJobName();

  /**
   * @return The ID of the current task. This ID number is 0-based.
   */
  int getTaskId();

  /**
   * @return The properties of the compute job.
   */
  Properties getJobProps();
}
