package com.linkedin.venice.service;

import com.linkedin.venice.exceptions.VeniceException;
import java.io.Closeable;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Blueprint for all Services initiated from Venice Server
 */
public abstract class AbstractVeniceService implements Closeable {
  protected final Logger logger = LogManager.getLogger(getClass());

  protected enum ServiceState {
    STOPPED, STARTING, STARTED
  }

  protected final AtomicReference<ServiceState> serviceState = new AtomicReference<>(ServiceState.STOPPED);

  public String getName() {
    return getClass().getSimpleName();
  }

  public boolean isRunning() {
    return serviceState.get().equals(ServiceState.STARTED);
  }

  public synchronized void start() {
    if (!serviceState.compareAndSet(ServiceState.STOPPED, ServiceState.STARTING)) {
      throw new VeniceException(
          "Service can only be started when in " + ServiceState.STOPPED + " state! Service:" + getName()
              + " Current state: " + serviceState.get());
    }

    try {
      logger.info("Starting {}", getName());
      long startTime = System.currentTimeMillis();
      if (startInner()) {
        serviceState.set(ServiceState.STARTED);
      } else {
        /**
         * N.B.: It is the Service implementer's responsibility to set {@link #serviceState} to
         * {@link ServiceState#STARTED} upon completion of the async work.
         */
        logger.info("Service {} may not be done starting. The process will finish asynchronously.", getName());
      }
      logger.info("{} startup took {}ms.", getName(), System.currentTimeMillis() - startTime);
    } catch (Exception e) {
      String msg = "Unable to start " + getName();
      logger.error(msg, e);
      throw new VeniceException(msg, e);
    }
  }

  public synchronized void stop() throws Exception {
    if (isRunning()) {
      logger.info("Stopping {}", getName());
      long startTime = System.currentTimeMillis();
      stopInner();
      serviceState.set(ServiceState.STOPPED);
      logger.info("{} shutdown took {}ms.", getName(), System.currentTimeMillis() - startTime);
    } else {
      logger.info("{} service has already been stopped.", getName());
    }
  }

  /**
   * @return true if the service is completely started,
   *         false if it is still starting asynchronously (in this case, it is the implementer's
   *         responsibility to set {@link #serviceState} to {@link ServiceState#STARTED} upon
   *         completion of the async work).
   * @throws Exception
   */
  public abstract boolean startInner() throws Exception;

  public abstract void stopInner() throws Exception;

  @Override
  public void close() {
    try {
      stop();
    } catch (Exception e) {
      String msg = "Unable to stop " + getName();
      logger.error(msg, e);
      throw new VeniceException(msg, e);
    }
  }
}
