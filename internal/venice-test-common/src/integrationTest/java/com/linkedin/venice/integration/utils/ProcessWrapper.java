package com.linkedin.venice.integration.utils;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.ExceptionUtils;
import com.linkedin.venice.utils.Utils;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;


/**
 * A class used for wrapping external systems and Venice components and
 * taking care of cleaning up after them when finished.
 */
public abstract class ProcessWrapper implements Closeable {
  private static final Logger LOGGER = LogManager.getLogger(ProcessWrapper.class);

  protected static final String DEFAULT_HOST_NAME = "localhost"; // Utils.getHostName();

  private final String serviceName;
  private final File dataDirectory;

  // Add a flag to avoid stopping a service that it's not started.
  private boolean isStarted;

  private boolean isRunning;

  private final Exception constructionCallstack;
  private final Thread shutdownHook;
  private boolean closeCalled;
  private Throwable closeThrowable = null;

  ProcessWrapper(String serviceName, File dataDirectory) {
    Utils.SUPPRESS_SYSTEM_EXIT.set(true);

    this.serviceName = serviceName;
    this.dataDirectory = dataDirectory;

    this.constructionCallstack = new VeniceException("Exception only for the sake of recording the construction stack");
    // We eliminate test framework stack trace elements, since they are not relevant to our leak debugging
    StackTraceElement[] stackTraceElements = this.constructionCallstack.getStackTrace();
    int firstUselessElement = -1;
    for (int i = 0; i < stackTraceElements.length; i++) {
      if (!stackTraceElements[i].getClassName().startsWith("com.linkedin")) {
        firstUselessElement = i;
        break;
      }
    }
    if (firstUselessElement > 0) {
      StackTraceElement[] prunedStackTraceElements = new StackTraceElement[firstUselessElement];
      System.arraycopy(stackTraceElements, 0, prunedStackTraceElements, 0, prunedStackTraceElements.length);
      this.constructionCallstack.setStackTrace(prunedStackTraceElements);
    }

    /**
     * Since {@link ZKServerWrapper} is using singleton mode, currently, there is no way to close it explicitly, but at exit.
     * So no need to report the following error for {@link ZkServerWrapper}, and this hook will be in charge of closing it properly.
     */
    this.shutdownHook = new Thread(() -> closeAudit("JVM shutdown time"));
    if (!(getClass().equals(ZkServerWrapper.class) && ZkServerWrapper.isSingleton())) {
      Runtime.getRuntime().addShutdownHook(shutdownHook);
    }
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
    } catch (Exception e) { /* VeniceClusterWrapper throws exceptions on getHost() and getPort() */
      return "No Address: " + e.getMessage();
    }
  }

  public String getServiceName() {
    return serviceName;
  }

  public String getComponentTagForLogging() {
    /**
     * We intentionally ignore hostname here since it will be always 'localhost' for the local integration tests.
     */
    return getServiceName() + "-" + getPort();
  }

  public static String getComponentTagPrefix(String prefix) {
    return prefix.isEmpty() ? "" : prefix + "-";
  }

  /**
   * This function should start the wrapped service AND block until the service is fully started.
   *
   * @throws Exception if there is any problem during the start up
   */
  protected synchronized void start() throws Exception {
    if (!isRunning) {
      isStarted = true;
      long startTime = System.currentTimeMillis();
      final AtomicReference<Exception> startException = new AtomicReference();
      /**
       * Spinning up a separate thread is to avoid polluting the {@link ThreadContext} of the current thread.
       */
      Thread startThread = new Thread(() -> {
        // Setup component tag for logging
        ThreadContext.put("component", getComponentTagForLogging());
        try {
          internalStart();
        } catch (Exception e) {
          startException.set(e);
        }
      });
      startThread.start();
      startThread.join();
      if (startException.get() != null) {
        throw startException.get();
      }
      LOGGER.info("{} startup took {} ms.", serviceName, System.currentTimeMillis() - startTime);
      isRunning = true;
    } else {
      LOGGER.info("{} service has already started.", serviceName);
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
    if (isStarted) {
      long startTime = System.currentTimeMillis();
      internalStop();
      LOGGER.info("{} shutdown took {} ms.", serviceName, System.currentTimeMillis() - startTime);
      isRunning = false;
      isStarted = false;
    } else {
      LOGGER.info("{} service has already been stopped.", serviceName);
    }
  }

  protected abstract void internalStop() throws Exception;

  protected synchronized void restart() throws Exception {
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

  public synchronized boolean isRunning() {
    return isRunning;
  }

  public synchronized void close() {
    if (closeCalled) {
      LOGGER.error(
          "Ignore duplicate attempt to close {} service running at {}",
          serviceName,
          getAddressForLogging(),
          new VeniceException("Duplicate close attempt."));
      return;
    }
    closeCalled = true;
    try {
      stop();
      Runtime.getRuntime().removeShutdownHook(shutdownHook);
    } catch (Throwable e) {
      closeThrowable = e;
      LOGGER.error("Failed to shutdown {} service running at {}", serviceName, getAddressForLogging(), e);
    }
    try {
      if (dataDirectory != null) {
        FileUtils.deleteDirectory(dataDirectory);
      }
    } catch (IOException e) {
      LOGGER.error("Failed to delete {}'s data directory: {}", serviceName, dataDirectory.getAbsolutePath(), e);
    }
  }

  private void closeAudit(String context) {
    if (!closeCalled) {
      System.out.println(
          getClass().getSimpleName() + " was not closed! Constructed at:\n"
              + ExceptionUtils.stackTraceToString(constructionCallstack));
    } else if (closeThrowable != null) {
      System.err.println(
          context + ": " + getClass().getSimpleName() + " was closed but failed to stop! Constructed at:\n"
              + ExceptionUtils.stackTraceToString(constructionCallstack) + "\n\nClose failure details:\n"
              + ExceptionUtils.stackTraceToString(closeThrowable));
    }
  }
}
