package com.linkedin.venice.hadoop.mapreduce.engine;

import com.linkedin.venice.exceptions.UndefinedPropertyException;
import com.linkedin.venice.hadoop.engine.EngineTaskConfigProvider;
import com.linkedin.venice.hadoop.utils.HadoopUtils;
import java.util.Properties;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskID;


/**
 * An implementation of {@link EngineTaskConfigProvider} to get information about a running job on the MapReduce engine.
 */
public class MapReduceEngineTaskConfigProvider implements EngineTaskConfigProvider {
  // Ideally, we should be using the new hadoop configs ("mapreduce." prefix) but the values of the new and old configs
  // are different. Continuing to use the old configs for now.
  public static final String MAPRED_TASK_ID_PROP_NAME = "mapred.task.id";
  public static final String MAPREDUCE_JOB_NAME_PROP_NAME = "mapreduce.job.name";
  private final JobConf jobConf;
  private Properties jobProps;

  public MapReduceEngineTaskConfigProvider(JobConf jobConf) {
    this.jobConf = jobConf;
  }

  @Override
  public String getJobName() {
    return getJobProps().getProperty(MAPREDUCE_JOB_NAME_PROP_NAME);
  }

  @Override
  public int getTaskId() {
    TaskAttemptID taskAttemptID = TaskAttemptID.forName(jobConf.get(MAPRED_TASK_ID_PROP_NAME));
    if (taskAttemptID == null) {
      throw new UndefinedPropertyException(
          MAPRED_TASK_ID_PROP_NAME + " not found in the " + JobConf.class.getSimpleName());
    }
    TaskID taskID = taskAttemptID.getTaskID();
    if (taskID == null) {
      throw new NullPointerException("taskAttemptID.getTaskID() is null");
    }
    return taskID.getId();
  }

  @Override
  public Properties getJobProps() {
    if (jobProps == null) {
      jobProps = HadoopUtils.getProps(jobConf);
    }
    return jobProps;
  }
}
