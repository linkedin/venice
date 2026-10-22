package com.linkedin.davinci;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.ClusterInfoProvider;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.Time;
import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


final class PushStatusClusterResolver implements Function<String, String>, Closeable {
  private static final Logger LOGGER = LogManager.getLogger(PushStatusClusterResolver.class);
  static final long RETRY_BACKOFF_MS = TimeUnit.SECONDS.toMillis(30);
  private final ClusterInfoProvider metadata;
  private final BiFunction<String, String, String> discovery;
  private final ExecutorService executor;
  private final Time time;
  private final Map<String, Resolution> resolutions = new HashMap<>();
  private boolean closed;

  PushStatusClusterResolver(
      ClusterInfoProvider metadata,
      boolean requestBasedMetadata,
      BiFunction<String, String, String> discovery) {
    this(
        metadata,
        discovery,
        requestBasedMetadata
            ? new ThreadPoolExecutor(
                1,
                1,
                0,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(1),
                new DaemonThreadFactory("DaVinci-PushStatus-Discovery"))
            : null,
        SystemTime.INSTANCE);
  }

  PushStatusClusterResolver(
      ClusterInfoProvider metadata,
      BiFunction<String, String, String> discovery,
      ExecutorService executor,
      Time time) {
    this.metadata = metadata;
    this.discovery = discovery;
    this.executor = executor;
    this.time = time;
  }

  @Override
  public synchronized String apply(String storeName) {
    if (closed) {
      return null;
    }
    String location = metadata.getVeniceCluster(storeName);
    if (location == null || location.isEmpty()) {
      throw new VeniceException("Cannot discover cluster for push status store of " + storeName);
    }
    if (executor == null) {
      return location;
    }
    // Request-based metadata reports a server D2 service, not the actual cluster.
    Resolution resolution = resolutions.computeIfAbsent(location, ignored -> new Resolution());
    if (resolution.cluster == null && !resolution.pending && time.getMilliseconds() >= resolution.retryAfterMs) {
      resolution.pending = true;
      try {
        executor.execute(() -> resolve(storeName, location, resolution));
      } catch (RejectedExecutionException e) {
        resolution.pending = false;
        resolution.retryAfterMs = time.getMilliseconds() + RETRY_BACKOFF_MS;
        LOGGER.warn("Push status cluster discovery is busy for store: {}", storeName, e);
      }
    }
    return resolution.cluster;
  }

  private void resolve(String storeName, String location, Resolution resolution) {
    String cluster = null;
    try {
      String discoveredCluster = discovery.apply(storeName, location);
      if (discoveredCluster == null || discoveredCluster.isEmpty()) {
        throw new VeniceException("Missing discovered cluster for " + storeName);
      }
      // A migrating store cannot populate the cache for its previous metadata location.
      if (location.equals(metadata.getVeniceCluster(storeName))) {
        cluster = discoveredCluster;
      }
    } catch (Exception e) {
      LOGGER.error("Failed to discover push status cluster for store: {} at location: {}", storeName, location, e);
    }
    synchronized (this) {
      if (!closed) {
        resolution.cluster = cluster;
        resolution.pending = false;
        resolution.retryAfterMs = time.getMilliseconds() + RETRY_BACKOFF_MS;
      }
    }
  }

  @Override
  public void close() {
    synchronized (this) {
      if (closed) {
        return;
      }
      closed = true;
      resolutions.clear();
      if (executor != null) {
        executor.shutdownNow();
      }
    }
    if (executor != null) {
      try {
        if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
          LOGGER.warn("Push status cluster discovery did not terminate during shutdown");
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private static class Resolution {
    private String cluster;
    private boolean pending;
    private long retryAfterMs;
  }
}
