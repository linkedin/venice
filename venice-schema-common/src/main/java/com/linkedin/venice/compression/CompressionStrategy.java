package com.linkedin.venice.compression;

import com.linkedin.venice.exceptions.VeniceException;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;


/**
 * Enums of the strategies used to compress/decompress Record's value
 */
public enum CompressionStrategy {
  NO_OP(0),
  GZIP(1);

  private final int value;

  private static final Map<Integer, CompressionStrategy> COMPRESSION_STRATEGY_MAP = getCompressionStrategyMap();

  CompressionStrategy(int value) {
    this.value = value;
  }

  public int getValue() {
    return value;
  }

  private static Map<Integer, CompressionStrategy> getCompressionStrategyMap() {
    Map<Integer, CompressionStrategy> intToTypeMap = new HashMap<>();
    for (CompressionStrategy strategy : CompressionStrategy.values()) {
      intToTypeMap.put(strategy.value, strategy);
    }

    return intToTypeMap;
  }

  public static CompressionStrategy valueOf(int value) {
    CompressionStrategy strategy = COMPRESSION_STRATEGY_MAP.get(value);
    if (strategy == null) {
      throw new VeniceException("Invalid compression type: " + value);
    }

    return strategy;
  }

  public static Optional<CompressionStrategy> optionalValueOf(int value) {
    return Optional.of(valueOf(value));
  }
}
