package com.linkedin.venice.meta;

import com.linkedin.venice.exceptions.VeniceException;


/**
 * Enums of strategies used to route the key to partition in Venice.
 */
public enum RoutingStrategy {
    /* Calculate a hashvalue by key then use consistent hash to get the partition. Details: https://en.wikipedia
    .org/wiki/Consistent_hashing*/
    CONSISTENT_HASH,
    /*Simple hash and modulo strategy. Calculate a hash value by key, partition=hahsvalue mod
    numberOfPartitions*/
    HASH;

    private static RoutingStrategy[] ALL_ROUTING_STRATEGIES = values();

    public static RoutingStrategy getRoutingStrategyFromOrdinal(int ordinal) {
        if (ordinal >= ALL_ROUTING_STRATEGIES.length) {
            throw new VeniceException("Invalid RoutingStrategy ordinal: " + ordinal);
        }
        return ALL_ROUTING_STRATEGIES[ordinal];
    }
}
