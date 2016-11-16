package com.linkedin.venice.hadoop.utils;

import com.linkedin.venice.utils.VeniceProperties;
import org.apache.hadoop.mapred.JobConf;

import java.util.Properties;

/**
 * Hadoop-specific utils.
 */
public class HadoopUtils {
  public static VeniceProperties getVeniceProps(JobConf conf) {
    Properties javaProps = new Properties();
    conf.forEach(entry -> javaProps.put(entry.getKey(), entry.getValue()));
    return new VeniceProperties(javaProps);
  }
}
