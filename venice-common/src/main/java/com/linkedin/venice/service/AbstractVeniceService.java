package com.linkedin.venice.service;

import com.linkedin.venice.exceptions.VeniceException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.log4j.Logger;


/**
 * Blueprint for all Services initiated from Venice Server
 *
 *
 */
public abstract class AbstractVeniceService {

  protected enum ServiceState {
    STOPPED,
    STARTING,
    STARTED
  }

  private final String serviceName;
  protected final AtomicReference<ServiceState> serviceState;
  private static final Logger logger = Logger.getLogger(AbstractVeniceService.class);

  public AbstractVeniceService() {
    this.serviceName = this.getClass().getSimpleName();
    this.serviceState = new AtomicReference<>(ServiceState.STOPPED);
  }

  public String getName() {
    return this.serviceName;
  }

  public void start() {
    try {
      if (!serviceState.compareAndSet(ServiceState.STOPPED, ServiceState.STARTING)) {
        throw new IllegalStateException("Service can only be started when in " + ServiceState.STOPPED +
            " state! Service:" + serviceName + " Current state: " + serviceState.get());
      }

      logger.info("Starting " + getName());
      if (startInner()) {
        serviceState.set(ServiceState.STARTED);
        logger.info("Service '" + serviceName + "' has finished starting!");
      } else {
        /**
         * N.B.: It is the Service implementer's responsibility to set {@link #serviceState} to
         * {@link ServiceState#STARTED} upon completion of the async work.
         */
        logger.info("Service '" + serviceName + "' may not be done starting. The process will finish asynchronously.");
      }
    } catch (Exception e) {
      String errMsg = "Error starting service: " + getName();
      logger.error(errMsg, e);
      throw new VeniceException(errMsg, e);
    }
  }

  public void stop()
      throws Exception {
    logger.info("Stopping " + getName());
    synchronized (this) {
      if (!isStarted()) {
        logger.info("This service is already stopped, ignoring duplicate attempt.");
        return;
      }
      stopInner();
      serviceState.set(ServiceState.STOPPED);
    }
  }

  public boolean isStarted() {
    return this.serviceState.get() == ServiceState.STARTED;
  }

  /**
   *
   * @return true if the service is completely started,
   *         false if it is still starting asynchronously (in this case, it is the implementer's
   *         responsibility to set {@link #serviceState} to {@link ServiceState#STARTED} upon
   *         completion of the async work).
   * @throws Exception
   */
  public abstract boolean startInner()
      throws Exception;

  public abstract void stopInner()
      throws Exception;

  public static void stopIfNotNull(AbstractVeniceService service){
    if (null != service){
      try {
        service.stop();
      } catch (Exception e) {
        logger.error("Unable to stop service: " + service.getName(), e);
      }
    }
  }
}
