package com.linkedin.venice.hadoop.utils;

import com.linkedin.venice.utils.VeniceProperties;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.JobConf;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Hadoop-specific utils.
 */
public class HadoopUtils {
  private static final Logger LOGGER = LogManager.getLogger(HadoopUtils.class);

  private HadoopUtils() {
  }

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
      Path p = new Path(path);
      FileSystem fs = p.getFileSystem(conf);
      fs.delete(p, recursive);
      fs.close();
    } catch (IOException e) {
      LOGGER.error("Failed to clean up the HDFS path: {}", path);
    }
  }

  /**
   * Create a temporary directory with the given name under the given path and set the specified permissions.
   * If the directory already exists, and the permissions are different from ones specified, the permissions will be
   * updated. If the directory already exists and the permissions are the same, nothing will be done.
   * @throws IOException
   */
  public static void createDirectoryWithPermission(Path path, FsPermission permission) throws IOException {
    LOGGER.info("Trying to create path {} with permission {}", path, permission);
    FileSystem fs = path.getFileSystem(new Configuration());
    // check if the path needs to be created
    if (fs.exists(path)) {
      LOGGER.info("path {} exists already", path);
      FsPermission currentPermission = fs.getFileStatus(path).getPermission();
      if (!currentPermission.equals(permission)) {
        LOGGER.info(
            "Current permission ({}) differs from expected permission ({}). Updating permissions.",
            currentPermission,
            permission);
        fs.setPermission(path, permission);
      }
    } else {
      LOGGER.info("Creating path {} with permission {}", path, permission);
      fs.mkdirs(path);
      // mkdirs(path,permission) didn't set the right permission when
      // tested in hdfs, so splitting it like this, it works!
      fs.setPermission(path, permission);
    }
  }

  /**
   * This function is a helper function to validate if a given file is a SequenceFile or not.
   * There is no utility function that can help fetch this info. This code has been mostly copied from the Apache Hadoop
   * project: https://github.com/apache/hadoop/blob/trunk/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/io/SequenceFile.java#L2022
   *
   * @return {@code true} if the {@param path} is a {@link SequenceFile}; {@code false} otherwise
   * @throws {@link IOException} if the input path is a directory, or on IO failure
   */
  public static boolean isSequenceFile(FileSystem fs, Path path) throws IOException {
    final byte VERSION_WITH_METADATA = (byte) 6;
    final byte[] VERSION = new byte[] { (byte) 'S', (byte) 'E', (byte) 'Q', VERSION_WITH_METADATA };

    byte[] versionBlock = new byte[VERSION.length];

    if (fs.isDirectory(path)) {
      throw new IOException("Input path " + path + " is not a file.");
    }

    try (DataInputStream in = fs.open(path)) {
      // Try to read sequence file header.
      try {
        in.readFully(versionBlock);
      } catch (Exception e) {
        return false;
      }
    }

    if ((versionBlock[0] != VERSION[0]) || (versionBlock[1] != VERSION[1]) || (versionBlock[2] != VERSION[2])) {
      return false;
    }

    // Set 'version'
    byte version = versionBlock[3];
    if (version > VERSION[3]) {
      return false;
    }

    return true;
  }
}
