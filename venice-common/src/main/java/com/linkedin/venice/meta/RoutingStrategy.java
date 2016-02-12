package com.linkedin.venice.meta;

/**
 * Enums of strategies used to route the key to partition in Venice.
 */
public enum RoutingStrategy {
    CONSISTENCY_HASH, HASH
}
