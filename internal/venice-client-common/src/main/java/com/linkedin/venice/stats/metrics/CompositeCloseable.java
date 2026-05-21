package com.linkedin.venice.stats.metrics;

import com.google.common.annotations.VisibleForTesting;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;


/**
 * Registry of {@link Closeable} resources owned by a single component. {@link #close()} drains in
 * reverse order via {@link MetricEntityStateUtils#closeQuietly}, is idempotent, and is final:
 * post-close {@link #register} calls close the resource immediately instead of tracking it.
 * Both methods are safe to call concurrently. {@link #NONE} is a no-op sentinel for test or
 * ad-hoc callsites.
 */
public class CompositeCloseable implements Closeable {
  /** No-op sentinel: registers nothing, closes nothing. Test / ad-hoc use only. */
  @VisibleForTesting
  public static final CompositeCloseable NONE = new CompositeCloseable() {
    @Override
    public <T extends Closeable> T register(T resource) {
      return resource;
    }

    @Override
    public void close() {
      /* no-op */
    }
  };

  private final List<Closeable> resources = new ArrayList<>();
  private boolean closed = false;

  /** Registers and returns the resource. Post-close registrations are closed immediately. */
  public <T extends Closeable> T register(T resource) {
    if (resource == null) {
      return null;
    }
    synchronized (resources) {
      if (closed) {
        MetricEntityStateUtils.closeQuietly(resource);
        return resource;
      }
      resources.add(resource);
    }
    return resource;
  }

  @Override
  public void close() {
    Closeable[] snapshot;
    synchronized (resources) {
      if (closed) {
        return;
      }
      closed = true;
      snapshot = resources.toArray(new Closeable[0]);
      resources.clear();
    }
    // Close outside the lock so a slow SDK close doesn't block concurrent register() calls.
    for (int i = snapshot.length - 1; i >= 0; i--) {
      MetricEntityStateUtils.closeQuietly(snapshot[i]);
    }
  }
}
