package com.linkedin.alpini.base.misc;

import java.lang.management.ManagementFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Utility methods for Processes.
 *
 * Created by acurtis on 2/8/16.
 */
public enum ProcessUtil {
  SINGLETON; // Effective Java, Item 3 - Enum singleton

  private static final Logger LOG = LogManager.getLogger(ProcessUtil.class);
  private static final int PID = Integer.parseInt(ManagementFactory.getRuntimeMXBean().getName().split("@")[0]);

  public static int getPid() {
    return PID;
  }

  public static void destroyQuietly(Process process) {
    try {
      if (process != null) {
        process.destroy();
      }
    } catch (Throwable ex) {
      LOG.warn("Failed to destroy process: {}", process, ex);
    }
  }
}
