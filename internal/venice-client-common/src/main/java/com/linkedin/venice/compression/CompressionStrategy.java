package com.linkedin.venice.compression;

import com.linkedin.venice.utils.EnumUtils;
import com.linkedin.venice.utils.VeniceEnumValue;
import java.util.List;


/**
 * Enums of the strategies used to compress/decompress Record's value
 */
public enum CompressionStrategy implements VeniceEnumValue {
  NO_OP(0, false), GZIP(1, true), @Deprecated
  ZSTD(2, true), ZSTD_WITH_DICT(3, true);

  private final int value;
  private final boolean compressionEnabled;

  private static final List<CompressionStrategy> TYPES = EnumUtils.getEnumValuesList(CompressionStrategy.class);

  CompressionStrategy(int value, boolean compressionEnabled) {
    this.value = value;
    this.compressionEnabled = compressionEnabled;
  }

  @Override
  public int getValue() {
    return value;
  }

  public boolean isCompressionEnabled() {
    return compressionEnabled;
  }

  public static CompressionStrategy valueOf(int value) {
    return EnumUtils.valueOf(TYPES, value, CompressionStrategy.class);
  }

  public static int getCompressionStrategyTypesArrayLength() {
    return TYPES.size();
  }
}
