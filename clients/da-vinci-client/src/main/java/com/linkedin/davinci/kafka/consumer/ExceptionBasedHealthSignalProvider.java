package com.linkedin.davinci.kafka.consumer;

import com.linkedin.venice.pubsub.PubSubHealthCategory;
import com.linkedin.venice.pubsub.PubSubHealthSignalProvider;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;


/**
 * A health signal provider that tracks PubSub exceptions. Any PubSub exception marks the
 * target as unhealthy — PubSub should not throw exceptions during normal operation, so a
 * single exception is sufficient to indicate a problem.
 *
 * <p>The exception state is cleared when the recovery probe reports success via
 * {@link #onProbeSuccess(String, PubSubHealthCategory)}.
 */
public class ExceptionBasedHealthSignalProvider implements PubSubHealthSignalProvider {
  private static final String NAME = "exception";

  // Outer map: pubSubAddress -> inner map: category -> true (present means unhealthy)
  private final Map<String, Set<PubSubHealthCategory>> unhealthyTargets = new ConcurrentHashMap<>();

  @Override
  public String getName() {
    return NAME;
  }

  /**
   * Record that a PubSub exception occurred for the given target. This immediately marks
   * the target as unhealthy from this provider's perspective.
   */
  public void recordException(String pubSubAddress, PubSubHealthCategory category) {
    unhealthyTargets.computeIfAbsent(pubSubAddress, k -> ConcurrentHashMap.newKeySet()).add(category);
  }

  @Override
  public boolean isUnhealthy(String pubSubAddress, PubSubHealthCategory category) {
    Set<PubSubHealthCategory> categories = unhealthyTargets.get(pubSubAddress);
    return categories != null && categories.contains(category);
  }

  @Override
  public void onProbeSuccess(String pubSubAddress, PubSubHealthCategory category) {
    Set<PubSubHealthCategory> categories = unhealthyTargets.get(pubSubAddress);
    if (categories != null) {
      categories.remove(category);
      if (categories.isEmpty()) {
        unhealthyTargets.remove(pubSubAddress);
      }
    }
  }
}
