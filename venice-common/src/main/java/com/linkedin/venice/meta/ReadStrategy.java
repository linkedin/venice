package com.linkedin.venice.meta;

import com.linkedin.venice.exceptions.VeniceException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;


/**
 * Enums of the strategies used to read data from multiple replications in Venice.
 */
public enum ReadStrategy {
  /*Read from any one online replication.*/
  ANY_OF_ONLINE(0),
  /*Read from two of replication and use the result returned by the faster one.*/
  FASTER_OF_TWO_ONLINE(1);

  public final int value;

  ReadStrategy(int v) {
    this.value = v;
  }

  private static final Map<Integer, ReadStrategy> idMapping = new HashMap<>();
  static {
    Arrays.stream(values()).forEach(s -> idMapping.put(s.value, s));
  }

  public static ReadStrategy getReadStrategyFromInt(int v) {
    ReadStrategy strategy = idMapping.get(v);
    if (strategy == null) {
      throw new VeniceException("Invalid ReadStrategy id: " + v);
    }
    return strategy;
  }
}
