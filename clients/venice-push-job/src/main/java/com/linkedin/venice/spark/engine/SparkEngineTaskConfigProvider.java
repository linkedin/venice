package com.linkedin.venice.spark.engine;

import static com.linkedin.venice.spark.SparkConstants.SPARK_APP_NAME_CONFIG;

import com.linkedin.venice.hadoop.engine.EngineTaskConfigProvider;
import java.util.Properties;
import org.apache.spark.TaskContext;


public class SparkEngineTaskConfigProvider implements EngineTaskConfigProvider {
  private final TaskContext taskContext;
  private final Properties jobProps;

  public SparkEngineTaskConfigProvider(Properties jobProps) {
    this.taskContext = TaskContext.get();

    this.jobProps = new Properties();
    this.jobProps.putAll(jobProps);
    this.jobProps.putAll(taskContext.getLocalProperties());
  }

  @Override
  public String getJobName() {
    return jobProps.getProperty(SPARK_APP_NAME_CONFIG);
  }

  @Override
  public int getTaskId() {
    return taskContext.partitionId();
  }

  @Override
  public Properties getJobProps() {
    return jobProps;
  }
}
