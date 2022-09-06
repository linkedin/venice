package com.linkedin.venice.hadoop;

import java.io.IOException;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;


public class DefaultHadoopJobClientProvider implements HadoopJobClientProvider {
  @Override
  public JobClient getJobClientFromConfig(JobConf jobConfig) throws IOException {
    return new JobClient(jobConfig);
  }
}
