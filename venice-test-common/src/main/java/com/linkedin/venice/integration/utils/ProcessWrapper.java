package com.linkedin.venice.integration.utils;

import com.linkedin.venice.exceptions.VeniceException;
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

  // Add a flag to avoid stopping a service that it's not running.
  private boolean isRunning;

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
   * Use this method for logging errors when we don't actually need an address and we cannot throw exceptions
   */
  public String getAddressForLogging() {
    try {
      return getAddress();
    } catch (Exception e){ /* VeniceClusterWrapper throws exceptions on getHost() and getPort() */
      return "No Address: " + e.getMessage();
    }
  }

  /**
   * This function should start the wrapped service AND block until the service is fully started.
   *
   * @throws Exception if there is any problem during the start up
   */
  protected synchronized void start() throws Exception {
    if(!isRunning) {
      internalStart();
      isRunning = true;
    } else {
      LOGGER.info(serviceName +" service has already started.");
    }
  }

  protected abstract void internalStart() throws Exception;

  /**
   * This function should stop the wrapped service. At this time, there is no expectation that the
   * service is fully stopped before this function returns (i.e.: it is acceptable if some things
   * finish asynchronously).
   *
   * @throws Exception if there are any problems while trying to stop the service (typically, these
   *                   exceptions will be ignored).
   */
  protected synchronized void stop() throws Exception {
    if (isRunning) {
      internalStop();
      isRunning = false;
    } else {
      LOGGER.info(serviceName + " service has already been stopped.");
    }
  }

  protected abstract void internalStop() throws Exception;

  protected synchronized void restart()
      throws Exception {
    if (!isRunning) {
      newProcess();
      start();
    } else {
      throw new VeniceException("Failed to restart " + serviceName + ", it's still running.");
    }
  }

  /**
   * Let each process wrapper to create a new process in it.
   * @throws Exception
   */
  protected abstract void newProcess() throws Exception;

  public synchronized boolean isRunning(){
    return isRunning;
  }

  public void close() {
    try {
      stop();
    } catch (Exception e) {
      LOGGER.error("Failed to shutdown " + serviceName + " service running at " + getAddressForLogging(), e);
    }
    try {
      if (dataDirectory != null && dataDirectory.exists()) {
        FileUtils.deleteDirectory(dataDirectory);
      }
    } catch (IOException e) {
      LOGGER.error("Failed to delete " + serviceName + "'s data directory: " + dataDirectory.getAbsolutePath(), e);
    }
  }
}
