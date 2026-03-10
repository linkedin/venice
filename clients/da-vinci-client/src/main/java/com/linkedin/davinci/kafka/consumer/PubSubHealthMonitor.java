package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.stats.PubSubHealthMonitorStats;
import com.linkedin.venice.pubsub.PubSubHealthCategory;
import com.linkedin.venice.pubsub.PubSubHealthChangeListener;
import com.linkedin.venice.pubsub.PubSubHealthSignalProvider;
import com.linkedin.venice.pubsub.PubSubHealthStatus;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.pubsub.manager.TopicManagerRepository;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.DaemonThreadFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Server-level singleton that tracks per-broker PubSub health and runs recovery probes for
 * unhealthy brokers. Health signals are provided by pluggable {@link PubSubHealthSignalProvider}
 * implementations.
 *
 * <p>The monitor has two main functions:
 * <ol>
 *   <li><b>Outage Detection:</b> Signal providers report unhealthy state. The first implementation
 *       ({@link ExceptionBasedHealthSignalProvider}) marks a broker unhealthy on any PubSub exception.</li>
 *   <li><b>Recovery Probe:</b> An independent background thread probes each unhealthy broker/service
 *       periodically. On success, the target transitions to HEALTHY and listeners are notified.</li>
 * </ol>
 */
public class PubSubHealthMonitor extends AbstractVeniceService {
  private static final Logger LOGGER = LogManager.getLogger(PubSubHealthMonitor.class);
  private static final int PROBE_TIMEOUT_SECONDS = 10;
  private final boolean enabled;
  private final int probeIntervalSeconds;
  private final TopicManagerRepository topicManagerRepository;

  // Signal providers
  private final ExceptionBasedHealthSignalProvider exceptionSignalProvider;
  private final List<PubSubHealthSignalProvider> signalProviders = new CopyOnWriteArrayList<>();

  // OTel metrics — null when monitor is disabled or OTel is not configured
  private volatile PubSubHealthMonitorStats stats;

  // Health status per (broker, category)
  private final Map<String, Map<PubSubHealthCategory, PubSubHealthStatus>> healthStatuses = new ConcurrentHashMap<>();

  // Listeners notified on health transitions
  private final List<PubSubHealthChangeListener> listeners = new CopyOnWriteArrayList<>();

  // Background threads — created in startInner() when enabled
  private ScheduledExecutorService probeExecutor;
  private ExecutorService listenerNotificationExecutor;
  private ExecutorService probeAsyncExecutor;

  // Probe topic — a well-known topic used to test broker reachability
  private volatile PubSubTopic probeTopic;

  public PubSubHealthMonitor(VeniceServerConfig serverConfig, TopicManagerRepository topicManagerRepository) {
    this.enabled = serverConfig.isPubSubHealthMonitorEnabled();
    this.probeIntervalSeconds = Math.max(1, serverConfig.getPubSubHealthProbeIntervalSeconds());
    this.topicManagerRepository = topicManagerRepository;

    // Register the built-in exception-based signal provider
    this.exceptionSignalProvider = new ExceptionBasedHealthSignalProvider();
    this.signalProviders.add(exceptionSignalProvider);

    // Executors are initialized in startInner() to avoid wasting threads when disabled
    this.probeExecutor = null;
    this.listenerNotificationExecutor = null;
    this.probeAsyncExecutor = null;
  }

  @Override
  public boolean startInner() {
    if (!enabled) {
      LOGGER.info("PubSub health monitor is disabled");
      return true;
    }

    LOGGER.info("Starting PubSub health monitor with probe interval {}s", probeIntervalSeconds);
    this.probeExecutor =
        Executors.newSingleThreadScheduledExecutor(new DaemonThreadFactory("PubSubHealthMonitor-Probe"));
    this.listenerNotificationExecutor =
        Executors.newSingleThreadExecutor(new DaemonThreadFactory("PubSubHealthMonitor-Listener"));
    this.probeAsyncExecutor =
        Executors.newSingleThreadExecutor(new DaemonThreadFactory("PubSubHealthMonitor-ProbeAsync"));
    probeExecutor
        .scheduleWithFixedDelay(this::runRecoveryProbe, probeIntervalSeconds, probeIntervalSeconds, TimeUnit.SECONDS);

    return true;
  }

  @Override
  public void stopInner() {
    shutdownExecutor(probeExecutor, "probe");
    shutdownExecutor(probeAsyncExecutor, "probe-async");
    shutdownExecutor(listenerNotificationExecutor, "listener-notification");
  }

  private void shutdownExecutor(ExecutorService executor, String name) {
    if (executor == null) {
      return;
    }
    executor.shutdown();
    try {
      if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
        executor.shutdownNow();
        LOGGER.warn("Forcefully shut down {} executor after timeout", name);
      }
    } catch (InterruptedException e) {
      executor.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Report a PubSub exception for the given target. This immediately marks the target as
   * UNHEALTHY if the monitor is enabled.
   */
  public void reportPubSubException(String pubSubAddress, PubSubHealthCategory category) {
    if (!enabled) {
      return;
    }

    exceptionSignalProvider.recordException(pubSubAddress, category);
    transitionToUnhealthyIfNeeded(pubSubAddress, category);
  }

  /**
   * Add a custom signal provider. Providers are queried during health status evaluation.
   */
  public void addSignalProvider(PubSubHealthSignalProvider provider) {
    signalProviders.add(provider);
  }

  /**
   * @return true if the target is healthy or if the monitor is disabled
   */
  public boolean isHealthy(String pubSubAddress, PubSubHealthCategory category) {
    if (!enabled) {
      return true;
    }
    return getHealthStatus(pubSubAddress, category) == PubSubHealthStatus.HEALTHY;
  }

  /**
   * @return the current health status of the target, defaulting to HEALTHY if not tracked
   */
  public PubSubHealthStatus getHealthStatus(String pubSubAddress, PubSubHealthCategory category) {
    Map<PubSubHealthCategory, PubSubHealthStatus> categoryStatuses = healthStatuses.get(pubSubAddress);
    if (categoryStatuses == null) {
      return PubSubHealthStatus.HEALTHY;
    }
    return categoryStatuses.getOrDefault(category, PubSubHealthStatus.HEALTHY);
  }

  public void registerListener(PubSubHealthChangeListener listener) {
    listeners.add(listener);
  }

  public void unregisterListener(PubSubHealthChangeListener listener) {
    listeners.remove(listener);
  }

  /**
   * Set the topic to use for recovery probes. This should be a well-known topic guaranteed
   * to exist (e.g., the server's own version topic or a dedicated health-check topic).
   */
  public void setProbeTopic(PubSubTopic topic) {
    this.probeTopic = topic;
  }

  public void setStats(PubSubHealthMonitorStats stats) {
    this.stats = stats;
  }

  /**
   * @return the number of currently unhealthy targets for the given category
   */
  public int getUnhealthyCount(PubSubHealthCategory category) {
    int count = 0;
    for (Map<PubSubHealthCategory, PubSubHealthStatus> categoryStatuses: healthStatuses.values()) {
      if (categoryStatuses.getOrDefault(category, PubSubHealthStatus.HEALTHY) == PubSubHealthStatus.UNHEALTHY) {
        count++;
      }
    }
    return count;
  }

  // Visible for testing
  ExceptionBasedHealthSignalProvider getExceptionSignalProvider() {
    return exceptionSignalProvider;
  }

  private void transitionToUnhealthyIfNeeded(String pubSubAddress, PubSubHealthCategory category) {
    Map<PubSubHealthCategory, PubSubHealthStatus> categoryStatuses =
        healthStatuses.computeIfAbsent(pubSubAddress, k -> new ConcurrentHashMap<>());

    // Atomically transition to UNHEALTHY (including HEALTHY -> UNHEALTHY) while
    // suppressing duplicate UNHEALTHY notifications.
    AtomicBoolean transitioned = new AtomicBoolean(false);
    categoryStatuses.compute(category, (k, previous) -> {
      if (previous == PubSubHealthStatus.UNHEALTHY) {
        return PubSubHealthStatus.UNHEALTHY; // Already unhealthy, no notification needed
      }

      // Verify at least one provider considers this target unhealthy
      boolean anyUnhealthy = false;
      String triggerName = null;
      for (PubSubHealthSignalProvider provider: signalProviders) {
        if (provider.isUnhealthy(pubSubAddress, category)) {
          anyUnhealthy = true;
          triggerName = provider.getName();
          break;
        }
      }

      if (!anyUnhealthy) {
        return previous; // Keep existing status (null or HEALTHY)
      }

      LOGGER.warn(
          "PubSub target marked UNHEALTHY: address={}, category={}, trigger={}",
          pubSubAddress,
          category,
          triggerName);
      transitioned.set(true);
      return PubSubHealthStatus.UNHEALTHY;
    });

    if (transitioned.get()) {
      if (stats != null) {
        stats.recordStateTransition(category);
      }
      notifyListeners(pubSubAddress, category, PubSubHealthStatus.UNHEALTHY);
    }
  }

  /**
   * Recovery probe task. Runs periodically on its own thread, independent of all SITs.
   * Probes each UNHEALTHY target and transitions to HEALTHY on success.
   */
  private void runRecoveryProbe() {
    PubSubTopic topic = probeTopic;
    if (topic == null) {
      LOGGER.debug("No probe topic configured, skipping recovery probe");
      return;
    }

    // Collect unhealthy targets to probe
    List<ProbeTarget> targets = new ArrayList<>();
    for (Map.Entry<String, Map<PubSubHealthCategory, PubSubHealthStatus>> entry: healthStatuses.entrySet()) {
      String address = entry.getKey();
      for (Map.Entry<PubSubHealthCategory, PubSubHealthStatus> categoryEntry: entry.getValue().entrySet()) {
        if (categoryEntry.getValue() == PubSubHealthStatus.UNHEALTHY) {
          targets.add(new ProbeTarget(address, categoryEntry.getKey()));
        }
      }
    }

    if (targets.isEmpty()) {
      return;
    }

    LOGGER.info("Running recovery probe for {} unhealthy targets", targets.size());

    for (ProbeTarget target: targets) {
      try {
        long probeStartMs = System.currentTimeMillis();
        boolean probeSuccess = probe(target.address, target.category, topic);
        long probeLatencyMs = System.currentTimeMillis() - probeStartMs;
        if (stats != null) {
          stats.recordProbeAttempt(target.category);
        }
        if (probeSuccess) {
          LOGGER.info("Recovery probe succeeded: address={}, category={}", target.address, target.category);

          // Notify all providers that the probe succeeded
          for (PubSubHealthSignalProvider provider: signalProviders) {
            provider.onProbeSuccess(target.address, target.category);
          }

          AtomicBoolean transitioned = new AtomicBoolean(false);
          healthStatuses.computeIfPresent(target.address, (addr, categoryStatuses) -> {
            categoryStatuses.compute(target.category, (k, previous) -> {
              // Re-check all providers under the compute lock
              for (PubSubHealthSignalProvider provider: signalProviders) {
                if (provider.isUnhealthy(target.address, target.category)) {
                  return PubSubHealthStatus.UNHEALTHY; // New exception arrived, stay unhealthy
                }
              }
              transitioned.set(true);
              return PubSubHealthStatus.HEALTHY;
            });

            // Clean up the outer map entry if all categories are now healthy
            if (transitioned.get()) {
              boolean allHealthy =
                  categoryStatuses.values().stream().allMatch(status -> status == PubSubHealthStatus.HEALTHY);
              if (allHealthy) {
                return null; // Remove this entry from healthStatuses
              }
            }
            return categoryStatuses;
          });

          if (stats != null) {
            stats.recordProbeSuccess(target.category);
            stats.recordProbeLatency(target.category, probeLatencyMs);
            if (transitioned.get()) {
              stats.recordStateTransition(target.category);
            }
          }
          if (transitioned.get()) {
            notifyListeners(target.address, target.category, PubSubHealthStatus.HEALTHY);
          }
        } else {
          LOGGER.debug("Recovery probe failed: address={}, category={}", target.address, target.category);

          for (PubSubHealthSignalProvider provider: signalProviders) {
            provider.onProbeFailure(target.address, target.category);
          }
          if (stats != null) {
            stats.recordProbeFailure(target.category);
            stats.recordProbeLatency(target.category, probeLatencyMs);
          }
        }
      } catch (Exception e) {
        LOGGER.debug("Recovery probe threw exception: address={}, category={}", target.address, target.category, e);

        for (PubSubHealthSignalProvider provider: signalProviders) {
          provider.onProbeFailure(target.address, target.category);
        }
        if (stats != null) {
          stats.recordProbeFailure(target.category);
        }
      }
    }
  }

  /**
   * Execute a probe for the given target. Uses TopicManager to check if we can retrieve
   * topic metadata, which sends a metadata request to the broker or metadata service.
   *
   * @return true if the probe succeeds (target is reachable)
   */
  private boolean probe(String address, PubSubHealthCategory category, PubSubTopic topic) {
    CompletableFuture<Boolean> future = null;
    try {
      TopicManager topicManager = topicManagerRepository.getTopicManager(address);
      future = CompletableFuture.supplyAsync(() -> topicManager.containsTopic(topic), probeAsyncExecutor);
      return future.get(PROBE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } catch (java.util.concurrent.TimeoutException e) {
      LOGGER.debug("Probe timed out for {} (category={})", address, category);
      if (future != null) {
        future.cancel(true);
      }
      return false;
    } catch (Exception e) {
      LOGGER.debug("Probe failed for {} (category={}): {}", address, category, e.getMessage());
      return false;
    }
  }

  private void notifyListeners(String pubSubAddress, PubSubHealthCategory category, PubSubHealthStatus newStatus) {
    if (listenerNotificationExecutor == null || listenerNotificationExecutor.isShutdown()) {
      return;
    }
    for (PubSubHealthChangeListener listener: listeners) {
      try {
        listenerNotificationExecutor.submit(() -> {
          try {
            listener.onHealthStatusChanged(pubSubAddress, category, newStatus);
          } catch (Exception e) {
            LOGGER.error("Error notifying health change listener", e);
          }
        });
      } catch (java.util.concurrent.RejectedExecutionException e) {
        LOGGER.warn("Cannot notify health change listener after executor shutdown for address={}", pubSubAddress);
        return;
      }
    }
  }

  private static class ProbeTarget {
    final String address;
    final PubSubHealthCategory category;

    ProbeTarget(String address, PubSubHealthCategory category) {
      this.address = address;
      this.category = category;
    }
  }
}
