package com.linkedin.venice.pubsub;

import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;


/**
 * Generic factory interface for creating PubSub consumers.
 * <p>
 * Concrete implementations should create and configure consumers for a specific PubSub system (e.g., Kafka, Pulsar).
 * <p>
 * Implementations must provide a public no-arg constructor to support reflective instantiation.
 */
public abstract class PubSubConsumerAdapterFactory<ADAPTER extends PubSubConsumerAdapter> implements Closeable {
  private final Map<String, Map<Object, Predicate<PubSubTopicPartition>>> monitoringRegistrations = new HashMap<>();

  /**
   * Constructor for PubSubConsumerAdapterFactory used mainly for reflective instantiation.
   */
  public PubSubConsumerAdapterFactory() {
    // no-op
  }

  /**
   * Creates a PubSub consumer adapter.
   *
   * @param context The context containing all dependencies and configurations required to create a consumer.
   * @return An instance of the PubSub consumer adapter.
   */
  public abstract ADAPTER create(PubSubConsumerAdapterContext context);

  /**
   * Whether these consumer properties enable adapter-owned automatic monitoring publication.
   * Factories must report their existing monitoring opt-in, or explicitly return false if they have no
   * automatic monitoring publisher. This is not a new consumer configuration.
   * The default rejects an undeclared capability rather than silently treating it as disabled.
   * This hook is queried during CDC monitoring setup; consumers without a monitoring gate are unchanged.
   */
  public boolean isAutomaticMonitoringEnabled(VeniceProperties consumerProperties) {
    throw new UnsupportedOperationException(getName() + " does not declare automatic monitoring support");
  }

  /**
   * Registers one client's eligibility. Clients of the same store are combined by union; closing a registration
   * releases only that client. An empty registration set leaves the store's monitoring unchanged.
   */
  public final synchronized Closeable registerMonitoringForStore(
      String storeName,
      Predicate<PubSubTopicPartition> positionFilter) {
    Map<Object, Predicate<PubSubTopicPartition>> registrations = monitoringRegistrations.get(storeName);
    Object registration = new Object();
    if (registrations == null) {
      Map<Object, Predicate<PubSubTopicPartition>> newRegistrations = new ConcurrentHashMap<>();
      newRegistrations.put(registration, positionFilter);
      try {
        configureMonitoringForStore(
            storeName,
            partition -> newRegistrations.isEmpty()
                || newRegistrations.values().stream().anyMatch(filter -> filter.test(partition)));
      } catch (RuntimeException | Error e) {
        newRegistrations.clear();
        throw e;
      }
      monitoringRegistrations.put(storeName, newRegistrations);
      registrations = newRegistrations;
    } else {
      registrations.put(registration, positionFilter);
    }
    Map<Object, Predicate<PubSubTopicPartition>> registeredFilters = registrations;
    return () -> registeredFilters.remove(registration);
  }

  /**
   * Installs a CDC monitoring-only position gate before creating ingestion consumers.
   * This must not filter ingestion or recovery checkpoints. Implementations must disable every native automatic
   * monitoring publication path and replace it with selected explicit positions. A factory that cannot do so
   * must fail rather than silently publish prefetched positions.
   * Pooled consumers apply each store's gate. Each predicate must preserve unrelated non-view positions;
   * CDC clients choose supported topics and validate exact version-topic ownership.
   */
  public void configureMonitoringForStore(String storeName, Predicate<PubSubTopicPartition> positionFilter) {
    throw new UnsupportedOperationException(getName() + " does not support gated CDC monitoring");
  }

  public abstract String getName();
}
