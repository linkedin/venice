package com.linkedin.venice.service;

import com.linkedin.venice.utils.Utils;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * A helper template for implementing VeniceService
 *
 *
 */
public abstract class AbstractService implements VeniceService {
  private final String serviceName;
  private final AtomicBoolean isStarted;

  public AbstractService(String serviceName) {
    this.serviceName = Utils.notNull(serviceName);
    isStarted = new AtomicBoolean(false);
  }

  @Override
  public String getName() {
    return this.serviceName;
  }

  @Override
  public void start()
      throws Exception {
    boolean isntStarted = isStarted.compareAndSet(false, true);
    if (!isntStarted) {
      throw new IllegalStateException("Service is already started!");
    }
    // TODO: Add logging
    startInner();
  }

  @Override
  public void stop()
      throws Exception {
    synchronized (this) {
      if (!isStarted()) {
        //TODO: add logging
        return;
      }
      stopInner();
    }
  }

  @Override
  public boolean isStarted() {
    return this.isStarted.get();
  }

  public abstract void startInner()
      throws Exception;

  public abstract void stopInner()
      throws Exception;
}
