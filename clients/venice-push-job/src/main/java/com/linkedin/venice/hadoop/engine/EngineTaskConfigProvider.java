package com.linkedin.venice.hadoop.engine;

import java.util.Properties;


/**
 * An interface to get information about a running job on any batch computing engine.
 */
public interface EngineTaskConfigProvider {
  String getJobName();

  int getTaskId();

  Properties getJobProps();
}
