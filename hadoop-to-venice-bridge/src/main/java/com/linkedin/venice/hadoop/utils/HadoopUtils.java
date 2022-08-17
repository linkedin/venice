package com.linkedin.venice.hadoop.utils;

import com.linkedin.venice.utils.VeniceProperties;
import java.io.IOException;
import java.util.Properties;
import org.apache.hadoop.mapred.JobConf;


/**
 * Hadoop-specific utils.
 */
public class HadoopUtils {
  public static VeniceProperties getVeniceProps(JobConf conf) {
    return new VeniceProperties(getProps(conf));
  }

  public static Properties getProps(JobConf conf) {
    Properties props = new Properties();
    conf.forEach(entry -> props.put(entry.getKey(), entry.getValue()));
    return props;
  }

  /**
   * Check if the path should be ignored. Currently only paths with "_log" are
   * ignored.
   */
  public static boolean shouldPathBeIgnored(org.apache.hadoop.fs.Path path) throws IOException {
    return path.getName().startsWith("_");
  }

  private HadoopUtils() {
  }
}
