package com.linkedin.venice.meta;

/**
 * Enums of strategies used to route the key to partition in Venice.
 */
public enum RoutingStrategy {
    /* Calculate a hashvalue by key then use consistent hash to get the partition. Details: https://en.wikipedia
    .org/wiki/Consistent_hashing*/
    CONSISTENT_HASH,
    /*Simple hash and modulo strategy. Calculate a hash value by key, partition=hahsvalue mod
    numberOfPartitions*/
    HASH
}
