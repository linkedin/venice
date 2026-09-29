package com.linkedin.venice.stats.metrics;

import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Groups the observable metric states of one component (e.g. a store's stats) so they can be
 * closed together when that component is genuinely retired, instead of reporting stale values forever.
 * Every async gauge factory requires a scope, so no gauge exists outside one.
 * Closing is idempotent; anything registered after close is closed immediately.
 */
public class MetricScope implements AutoCloseable {
  private static final Logger LOGGER = LogManager.getLogger(MetricScope.class);
  private final List<AutoCloseable> resources = new ArrayList<>();
  private boolean closed = false;

  public <T extends AutoCloseable> T register(T resource) {
    boolean closeNow;
    synchronized (this) {
      closeNow = closed;
      if (!closeNow) {
        resources.add(resource);
      }
    }
    if (closeNow) {
      closeResource(resource);
    }
    return resource;
  }

  public synchronized boolean isClosed() {
    return closed;
  }

  @Override
  public void close() {
    List<AutoCloseable> toClose;
    synchronized (this) {
      if (closed) {
        return;
      }
      closed = true;
      toClose = new ArrayList<>(resources);
    }
    for (AutoCloseable resource: toClose) {
      closeResource(resource);
    }
  }

  private void closeResource(AutoCloseable resource) {
    try {
      resource.close();
    } catch (Exception e) {
      LOGGER.warn("Failed to close metric resource", e);
    }
  }
}
