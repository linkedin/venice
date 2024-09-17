package com.linkedin.venice.compression;

import com.linkedin.alpini.base.misc.CollectionUtil;
import com.linkedin.venice.utils.VeniceEnumValueTest;
import java.util.Map;


public class CompressionStrategyTest extends VeniceEnumValueTest<CompressionStrategy> {
  public CompressionStrategyTest() {
    super(CompressionStrategy.class);
  }

  @Override
  protected Map<Integer, CompressionStrategy> expectedMapping() {
    return CollectionUtil.<Integer, CompressionStrategy>mapBuilder()
        .put(0, CompressionStrategy.NO_OP)
        .put(1, CompressionStrategy.GZIP)
        .put(2, CompressionStrategy.ZSTD)
        .put(3, CompressionStrategy.ZSTD_WITH_DICT)
        .build();
  }
}
