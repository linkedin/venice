package com.linkedin.alpini.base.registry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * @author Antony T Curtis &lt;acurtis@linkedin.com&gt;
 */
public class SyncShutdownableAdapter extends AbstractShutdownableResource<SyncShutdownable>
    implements ResourceRegistry.Sync {
  private static final Logger LOG = LogManager.getLogger(SyncShutdownableAdapter.class);

  public static SyncShutdownableAdapter wrap(SyncShutdownable resource) {
    if (resource instanceof ResourceRegistry.ShutdownFirst) {
      return new First(resource);
    }
    if (resource instanceof ResourceRegistry.ShutdownLast) {
      return new Last(resource);
    }
    return new SyncShutdownableAdapter(resource);
  }

  private static class First extends SyncShutdownableAdapter implements ResourceRegistry.ShutdownFirst {
    First(SyncShutdownable resource) {
      super(resource);
    }
  }

  private static class Last extends SyncShutdownableAdapter implements ResourceRegistry.ShutdownLast {
    Last(SyncShutdownable resource) {
      super(resource);
    }
  }

  private SyncShutdownableAdapter(final SyncShutdownable resource) {
    super(resource);
  }

  @Override
  protected Runnable constructShutdown(final SyncShutdownable resource) {
    return () -> {
      try {
        signalShutdownInitiated();
        resource.shutdownSynchronously();
      } catch (Throwable e) {
        LOG.warn("Shutdown interrupted", e);
      }
    };
  }
}
