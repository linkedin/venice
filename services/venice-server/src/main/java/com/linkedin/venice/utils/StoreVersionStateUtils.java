package com.linkedin.venice.utils;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;


public class StoreVersionStateUtils {
  public static boolean isChunked(StoreVersionState svs) {
    return svs == null ? false : svs.getChunked();
  }

  public static CompressionStrategy getCompressionStrategy(StoreVersionState svs) {
    return svs == null ? CompressionStrategy.NO_OP : CompressionStrategy.valueOf(svs.getCompressionStrategy());
  }
}
