package com.linkedin.venice.service;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.Utils;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.log4j.Logger;


/**
 * Blueprint for all Services initiated from Venice Server
 *
 *
 */
public abstract class AbstractVeniceService {
  private final String serviceName;
  private final AtomicBoolean isStarted;
  private static final Logger logger = Logger.getLogger(AbstractVeniceService.class);

  public AbstractVeniceService(String serviceName) {
    this.serviceName = Utils.notNull(serviceName);
    isStarted = new AtomicBoolean(false);
  }

  public String getName() {
    return this.serviceName;
  }

  public void start() {
    try {
      boolean isntStarted = isStarted.compareAndSet(false, true);
      if (!isntStarted) {
        throw new IllegalStateException("Service is already started!");
      }
      logger.info("Starting " + getName());
      startInner();
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
      isStarted.set(false);
    }
  }

  public boolean isStarted() {
    return this.isStarted.get();
  }

  public abstract void startInner()
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
