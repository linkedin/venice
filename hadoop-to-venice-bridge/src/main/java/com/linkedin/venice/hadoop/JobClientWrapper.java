package com.linkedin.venice.hadoop;

import java.io.IOException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;


public interface JobClientWrapper {
  RunningJob runJobWithConfig(JobConf jobConf) throws IOException;
}
