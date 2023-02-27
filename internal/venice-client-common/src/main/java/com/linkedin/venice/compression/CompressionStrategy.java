package com.linkedin.venice.compression;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.EnumUtils;
import com.linkedin.venice.utils.VeniceEnumValue;


/**
 * Enums of the strategies used to compress/decompress Record's value
 */
public enum CompressionStrategy implements VeniceEnumValue {
  NO_OP(0, false), GZIP(1, true), @Deprecated
  ZSTD(2, true), ZSTD_WITH_DICT(3, true);

  private final int value;
  private final boolean compressionEnabled;

  private static final CompressionStrategy[] TYPES_ARRAY = EnumUtils.getEnumValuesArray(CompressionStrategy.class);

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

  public static CompressionStrategy valueOf(int value) {
    try {
      return TYPES_ARRAY[value];
    } catch (IndexOutOfBoundsException e) {
      throw new VeniceException("Invalid compression strategy: " + value);
    }
  }

  public static int getCompressionStrategyTypesArrayLength() {
    return TYPES_ARRAY.length;
  }
}
