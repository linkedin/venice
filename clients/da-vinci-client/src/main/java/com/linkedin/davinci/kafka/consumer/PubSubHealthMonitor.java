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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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

  // Background threads — created in constructor, scheduled in startInner()
  private final ScheduledExecutorService probeExecutor;
  private final ExecutorService listenerNotificationExecutor;

  // Probe topic — a well-known topic used to test broker reachability
  private volatile PubSubTopic probeTopic;

  public PubSubHealthMonitor(VeniceServerConfig serverConfig, TopicManagerRepository topicManagerRepository) {
    this.enabled = serverConfig.isPubSubHealthMonitorEnabled();
    this.probeIntervalSeconds = serverConfig.getPubSubHealthProbeIntervalSeconds();
    this.topicManagerRepository = topicManagerRepository;

    // Register the built-in exception-based signal provider
    this.exceptionSignalProvider = new ExceptionBasedHealthSignalProvider();
    this.signalProviders.add(exceptionSignalProvider);

    this.probeExecutor =
        Executors.newSingleThreadScheduledExecutor(new DaemonThreadFactory("PubSubHealthMonitor-Probe"));
    this.listenerNotificationExecutor =
        Executors.newSingleThreadExecutor(new DaemonThreadFactory("PubSubHealthMonitor-Listener"));
  }

  @Override
  public boolean startInner() {
    if (!enabled) {
      LOGGER.info("PubSub health monitor is disabled");
      return true;
    }

    LOGGER.info("Starting PubSub health monitor with probe interval {}s", probeIntervalSeconds);
    probeExecutor
        .scheduleWithFixedDelay(this::runRecoveryProbe, probeIntervalSeconds, probeIntervalSeconds, TimeUnit.SECONDS);

    return true;
  }

  @Override
  public void stopInner() {
    probeExecutor.shutdownNow();
    listenerNotificationExecutor.shutdownNow();
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
   * @return the number of currently unhealthy targets across all categories
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
    final boolean[] transitioned = { false };
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
      transitioned[0] = true;
      return PubSubHealthStatus.UNHEALTHY;
    });

    if (transitioned[0]) {
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
        boolean probeSuccess = probe(target.address, target.category, topic);
        if (probeSuccess) {
          LOGGER.info("Recovery probe succeeded: address={}, category={}", target.address, target.category);

          // Notify all providers that the probe succeeded
          for (PubSubHealthSignalProvider provider: signalProviders) {
            provider.onProbeSuccess(target.address, target.category);
          }

          // Transition to HEALTHY
          Map<PubSubHealthCategory, PubSubHealthStatus> categoryStatuses = healthStatuses.get(target.address);
          if (categoryStatuses != null) {
            categoryStatuses.put(target.category, PubSubHealthStatus.HEALTHY);
          }

          if (stats != null) {
            stats.recordProbeSuccess(target.category);
            stats.recordStateTransition(target.category);
          }
          notifyListeners(target.address, target.category, PubSubHealthStatus.HEALTHY);
        } else {
          LOGGER.debug("Recovery probe failed: address={}, category={}", target.address, target.category);

          for (PubSubHealthSignalProvider provider: signalProviders) {
            provider.onProbeFailure(target.address, target.category);
          }
          if (stats != null) {
            stats.recordProbeFailure(target.category);
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
    try {
      TopicManager topicManager = topicManagerRepository.getTopicManager(address);
      return topicManager.containsTopic(topic);
    } catch (Exception e) {
      LOGGER.debug("Probe failed for {} (category={}): {}", address, category, e.getMessage());
      return false;
    }
  }

  private void notifyListeners(String pubSubAddress, PubSubHealthCategory category, PubSubHealthStatus newStatus) {
    if (listenerNotificationExecutor.isShutdown()) {
      return;
    }
    for (PubSubHealthChangeListener listener: listeners) {
      listenerNotificationExecutor.submit(() -> {
        try {
          listener.onHealthStatusChanged(pubSubAddress, category, newStatus);
        } catch (Exception e) {
          LOGGER.error("Error notifying health change listener", e);
        }
      });
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
