package com.linkedin.venice.hadoop;

import java.io.IOException;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;


interface HadoopJobClientProvider {
  JobClient getJobClientFromConfig(JobConf job) throws IOException;
}
