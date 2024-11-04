package com.linkedin.venice.pubsub.api;

import java.util.Locale;


/**
 * This enum is equivalent to Kafka's SecurityProtocol enum.
 *
 * We need this abstraction because Kafka's enum is present in two different namespaces, which are different between
 * LinkedIn's fork and the Apache fork.
 */
public enum PubSubSecurityProtocol {
  /** Un-authenticated, non-encrypted channel */
  PLAINTEXT,
  /** SSL channel */
  SSL,
  /** SASL authenticated, non-encrypted channel */
  SASL_PLAINTEXT,
  /** SASL authenticated, SSL channel */
  SASL_SSL;

  public static PubSubSecurityProtocol forName(String name) {
    return PubSubSecurityProtocol.valueOf(name.toUpperCase(Locale.ROOT));
  }
}
