package com.linkedin.venice.pubsub;

import com.linkedin.venice.pubsub.api.PubSubAdminAdapter;
import java.io.Closeable;


/**
 * Generic factory interface for creating PubSub system-specific admin instances.
 * <p>
 * Concrete implementations of this interface are expected to provide the logic for creating
 * and instantiating admin components tailored to a specific PubSub system (e.g., Kafka, Pulsar).
 * <p>
 * Implementations must provide a public no-arg constructor for reflective instantiation.
 */
public abstract class PubSubAdminAdapterFactory<ADAPTER extends PubSubAdminAdapter> implements Closeable {
  /**
   * Constructor for PubSubAdminAdapterFactory used mainly for reflective instantiation.
   */
  public PubSubAdminAdapterFactory() {
    // no-op
  }

  /**
   * Creates a PubSub admin adapter.
   * @param adminAdapterContext The context containing all dependencies and configurations required to create an admin adapter.
   * @return An instance of the PubSub admin adapter.
   */
  public abstract ADAPTER create(PubSubAdminAdapterContext adminAdapterContext);

  public abstract String getName();
}
