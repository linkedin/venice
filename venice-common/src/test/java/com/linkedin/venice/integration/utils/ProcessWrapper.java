package com.linkedin.venice.integration.utils;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

/**
 * A class used for wrapping external systems and Venice components and
 * taking care of cleaning up after them when finished.
 */
public abstract class ProcessWrapper implements Closeable {
  private static final Logger LOGGER = Logger.getLogger(ProcessWrapper.class);

  protected static final String DEFAULT_HOST_NAME = "localhost"; // Utils.getHostName();

  private final String serviceName;
  private final File dataDirectory;

  ProcessWrapper(String serviceName, File dataDirectory) {
    this.serviceName = serviceName;
    this.dataDirectory = dataDirectory;
  }

  /**
   * @return the host of the service
   */
  public abstract String getHost();

  /**
   * @return the port of the service
   */
  public abstract int getPort();

  /**
   * @return the address of the service in the form of "hostname:port"
   */
  public String getAddress() {
    return getHost() + ":" + getPort();
  }

  /**
   * This function should start the wrapped service AND block until the service is fully started.
   *
   * @throws Exception if there is any problem during the start up
   */
  protected abstract void start() throws Exception;

  /**
   * This function should stop the wrapped service. At this time, there is no expectation that the
   * service is fully stopped before this function returns (i.e.: it is acceptable if some things
   * finish asynchronously).
   *
   * @throws Exception if there are any problems while trying to stop the service (typically, these
   *                   exceptions will be ignored).
   */
  protected abstract void stop() throws Exception;

  public void close() {
    try {
      stop();
    } catch (Exception e) {
      LOGGER.error("Failed to shutdown " + serviceName + " service running at " + getAddress(), e);
    }
    try {
      if (dataDirectory != null) {
        FileUtils.deleteDirectory(dataDirectory);
      }
    } catch (IOException e) {
      LOGGER.error("Failed to delete " + serviceName + "'s data directory: " + dataDirectory.getAbsolutePath(), e);
    }
  }
}
