package com.linkedin.venice.stats.metrics;

import com.google.common.annotations.VisibleForTesting;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;


/**
 * Registry of {@link Closeable} resources owned by a single component. An owner class typically
 * holds one instance as a {@code private final} field and drains it from its shutdown method.
 * Resources register through either:
 * <ul>
 *   <li>the {@code registry} parameter on {@code MetricEntityState*} / {@code AsyncMetricEntityState*}
 *       {@code create()} factories (framework path), or</li>
 *   <li>a direct {@link #register(Closeable)} call (owner-class path; e.g.
 *       {@code statsCloseables.register(new ThreadPoolStats(...))}).</li>
 * </ul>
 *
 * <p>{@link #close()} drains the registry in reverse order of registration. Errors are logged and
 * swallowed by {@link MetricEntityStateUtils#closeQuietly} so a misbehaving wrapper cannot block
 * shutdown. {@link #close()} is idempotent and final: after it returns, subsequent
 * {@link #register(Closeable)} calls close the resource immediately rather than tracking it.
 *
 * <p>{@link #register(Closeable)} and {@link #close()} are safe to call concurrently;
 * registrations are serialised via the internal lock.
 *
 * <p>{@link #NONE} is a no-op sentinel for test or ad-hoc callsites where no lifecycle applies.
 * Production code should always own a real {@link CompositeCloseable}.
 */
public class CompositeCloseable implements Closeable {
  /**
   * No-op sentinel: registers nothing, closes nothing. Test / ad-hoc use only — resources passed
   * here are not tracked and never closed. Production code should hold a real
   * {@link CompositeCloseable} (typically a {@code private final} field on the owner class).
   */
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

  /**
   * Registers and returns the resource for fluent assignment.
   *
   * <p>If this registry has already been closed, the resource is closed immediately (best-effort)
   * and the call returns the (now-closed) resource. This prevents post-close registrations from
   * silently leaking SDK callbacks when shutdown ordering is racy.
   */
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
