package com.linkedin.venice.compression;

import com.linkedin.venice.exceptions.VeniceException;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;


/**
 * Enums of the strategies used to compress/decompress Record's value
 */
public enum CompressionStrategy {
  NO_OP(0, false),
  GZIP(1, true),
  // Value 2 has been used in the past and we should not use it in the future.
  ZSTD_WITH_DICT(3, true);

  private final int value;
  private final boolean compressionEnabled;

  private static final Map<Integer, CompressionStrategy> COMPRESSION_STRATEGY_MAP = getCompressionStrategyMap();

  CompressionStrategy(int value, boolean compressionEnabled) {
    this.value = value;
    this.compressionEnabled = compressionEnabled;
  }

  public int getValue() {
    return value;
  }

  public boolean isCompressionEnabled() {
    return compressionEnabled;
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
