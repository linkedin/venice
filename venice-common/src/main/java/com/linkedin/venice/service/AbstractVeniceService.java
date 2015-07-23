package com.linkedin.venice.service;

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

  public void start()
      throws Exception {
    boolean isntStarted = isStarted.compareAndSet(false, true);
    if (!isntStarted) {
      throw new IllegalStateException("Service is already started!");
    }
    logger.info("Starting " + getName());
    startInner();
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
}
