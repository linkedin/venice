package com.linkedin.venice.hadoop;

import java.io.IOException;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;


public class DefaultJobClientWrapper implements JobClientWrapper {
  @Override
  public RunningJob runJobWithConfig(JobConf jobConf) throws IOException {
    return JobClient.runJob(jobConf);
  }
}
