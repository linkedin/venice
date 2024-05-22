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
  PLAINTEXT(0, "PLAINTEXT"),
  /** SSL channel */
  SSL(1, "SSL"),
  /** SASL authenticated, non-encrypted channel */
  SASL_PLAINTEXT(2, "SASL_PLAINTEXT"),
  /** SASL authenticated, SSL channel */
  SASL_SSL(3, "SASL_SSL");

  /** The permanent and immutable id of a security protocol -- this can't change, and must match kafka.cluster.SecurityProtocol  */
  public final short id;

  /** Name of the security protocol. This may be used by client configuration. */
  public final String name;

  PubSubSecurityProtocol(int id, String name) {
    this.id = (short) id;
    this.name = name;
  }

  public static PubSubSecurityProtocol forName(String name) {
    return PubSubSecurityProtocol.valueOf(name.toUpperCase(Locale.ROOT));
  }
}
