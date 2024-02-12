package com.linkedin.venice.hadoop.mapreduce.engine;

import java.io.IOException;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;


public interface HadoopJobClientProvider {
  JobClient getJobClientFromConfig(JobConf job) throws IOException;
}
