package com.linkedin.venice.hadoop;

import java.io.IOException;
import java.util.function.Consumer;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;


public abstract class AbstractTestVeniceMapper<M extends AbstractVeniceMapper> extends AbstractTestVeniceMR {
  protected abstract M newMapper();

  protected M getMapper(int numReducers, int taskId) throws IOException {
    Consumer<JobConf> noOpJobConfigurator = jobConf -> {};
    return getMapper(numReducers, taskId, noOpJobConfigurator);
  }

  protected M getMapper(int numReducers, int taskId, Consumer<JobConf> jobConfigurator) throws IOException {
    M mapper = newMapper();
    JobConf jobConf = setupJobConf(numReducers, taskId);
    jobConfigurator.accept(jobConf);
    mapper.configure(jobConf);
    return mapper;
  }

  /**
   * For the mapper with task ID 0, the number of invocations is {@param numReducers} + 1, because on the
   * first call to {@link VeniceAvroMapper#map(Object, Object, OutputCollector, Reporter)} there will be an
   * empty key written to the {@link OutputCollector} for each partition (i.e. for each reducer), and then
   * a call to write the first real K/V payload. For the tasks ID > 0, only real K/V payloads are written.
   */
  protected int getNumberOfCollectorInvocationForFirstMapInvocation(int numReducers, int taskId) {
    if (taskId == 0) {
      return numReducers + 1;
    }
    return 1;
  }

  protected JobConf setupJobConf(int numReducers, int taskId) {
    JobConf jobConf = setupJobConf();
    jobConf.setNumReduceTasks(numReducers);
    TaskAttemptID taskAttemptID = new TaskAttemptID("200707121733", 3, TaskType.MAP, taskId, 0);
    jobConf.set(AbstractVeniceMapper.MAPRED_TASK_ID_PROP_NAME, taskAttemptID.toString());
    return jobConf;
  }
}
