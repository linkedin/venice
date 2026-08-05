package com.linkedin.venice.controller;

/**
 * Provisions externally managed PubSub encryption keys for Venice stores.
 *
 * <p>Implementations must be thread-safe and idempotent. This synchronous contract intentionally allows version
 * creation to fail closed when key provisioning does not complete successfully.
 */
@FunctionalInterface
public interface PubSubEncryptionKeyProvider {
  /**
   * @return the canonical, non-blank URN of the provisioned key
   */
  String getOrCreatePubSubEncryptionKeyUrn(String clusterName, String storeName);
}
