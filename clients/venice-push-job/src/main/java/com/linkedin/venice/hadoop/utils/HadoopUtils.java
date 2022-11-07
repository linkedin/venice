package com.linkedin.venice.hadoop.utils;

import com.linkedin.venice.utils.VeniceProperties;
import java.io.IOException;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Hadoop-specific utils.
 */
public class HadoopUtils {
  private static final Logger LOGGER = LogManager.getLogger(HadoopUtils.class);

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

  /**
   * Silently clean up the given path on HDFS. If fails, it will ignore the failure and log a message.
   * @param path
   * @param recursive - see {@link FileSystem#delete(Path, boolean)}
   */
  public static void cleanUpHDFSPath(String path, boolean recursive) {
    Configuration conf = new Configuration();
    try {
      FileSystem fs = FileSystem.get(conf);
      Path p = new Path(path);
      fs.delete(p, recursive);
      fs.close();
    } catch (IOException e) {
      LOGGER.error("Failed to clean up the HDFS path: {}", path);
    }
  }

  private HadoopUtils() {
  }
}
