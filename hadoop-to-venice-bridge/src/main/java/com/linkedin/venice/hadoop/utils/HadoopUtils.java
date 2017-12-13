package com.linkedin.venice.hadoop.utils;

import com.linkedin.events.prop.Prop;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.IOException;
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

  private HadoopUtils() {}
}
