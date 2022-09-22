package com.linkedin.alpini.base.registry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * @author Antony T Curtis &lt;acurtis@linkedin.com&gt;
 */
class ShutdownableAdapter extends AbstractShutdownableResource<Shutdownable> {
  private static final Logger LOG = LogManager.getLogger(ShutdownableAdapter.class);

  public static ShutdownableAdapter wrap(Shutdownable resource) {
    if (resource instanceof ResourceRegistry.ShutdownFirst) {
      return new First(resource);
    }
    if (resource instanceof ResourceRegistry.ShutdownLast) {
      return new Last(resource);
    }
    return new ShutdownableAdapter(resource);
  }

  private static class First extends ShutdownableAdapter implements ResourceRegistry.ShutdownFirst {
    First(Shutdownable resource) {
      super(resource);
    }
  }

  private static class Last extends ShutdownableAdapter implements ResourceRegistry.ShutdownLast {
    Last(Shutdownable resource) {
      super(resource);
    }
  }

  private ShutdownableAdapter(Shutdownable resource) {
    super(resource);
  }

  protected Runnable constructShutdown(final Shutdownable resource) {
    return () -> {
      try {
        resource.shutdown();
        signalShutdownInitiated();
        resource.waitForShutdown();
      } catch (Throwable e) {
        LOG.warn("Shutdown interrupted", e);
      }
    };
  }
}
