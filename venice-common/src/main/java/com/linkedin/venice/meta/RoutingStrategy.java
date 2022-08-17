package com.linkedin.venice.meta;

import com.linkedin.venice.exceptions.VeniceException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;


/**
 * Enums of strategies used to route the key to partition in Venice.
 */
public enum RoutingStrategy {
  /* Calculate a hashvalue by key then use consistent hash to get the partition. Details: https://en.wikipedia
  .org/wiki/Consistent_hashing*/
  CONSISTENT_HASH(0),
  /*Simple hash and modulo strategy. Calculate a hash value by key, partition=hahsvalue mod
  numberOfPartitions*/
  HASH(1);

  private static RoutingStrategy[] ALL_ROUTING_STRATEGIES = values();

  public final int value;

  RoutingStrategy(int v) {
    this.value = v;
  }

  private static final Map<Integer, RoutingStrategy> idMapping = new HashMap<>();
  static {
    Arrays.stream(values()).forEach(s -> idMapping.put(s.value, s));
  }

  public static RoutingStrategy getRoutingStrategyFromInt(int v) {
    RoutingStrategy strategy = idMapping.get(v);
    if (strategy == null) {
      throw new VeniceException("Invalid RoutingStrategy id: " + v);
    }
    return strategy;
  }
}
