package com.linkedin.venice.listener.response.stats;

import com.linkedin.venice.stats.ServerHttpRequestStats;
import com.linkedin.venice.stats.dimensions.HttpResponseStatusCodeCategory;
import com.linkedin.venice.stats.dimensions.HttpResponseStatusEnum;
import com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory;
import com.linkedin.venice.utils.DoubleAndBooleanConsumer;
import it.unimi.dsi.fastutil.ints.IntList;
import java.util.function.DoubleConsumer;
import java.util.function.IntConsumer;


public class ResponseStatsUtil {
  public static void recordKeyValueSizes(
      ServerHttpRequestStats stats,
      IntList keySizes,
      IntList valueSizes,
      HttpResponseStatusEnum statusEnum,
      HttpResponseStatusCodeCategory statusCategory,
      VeniceResponseStatusCategory veniceCategory) {
    for (int i = 0; i < valueSizes.size(); i++) {
      int valueSize = valueSizes.getInt(i);
      if (valueSize > 0) {
        stats.recordValueSizeInByte(statusEnum, statusCategory, veniceCategory, valueSize);
      }
    }
    for (int i = 0; i < keySizes.size(); i++) {
      stats.recordKeySizeInByte(keySizes.getInt(i));
    }
  }

  public static void consumeIntIfAbove(IntConsumer consumer, int value, int threshold) {
    if (value > threshold) {
      consumer.accept(value);
    }
  }

  public static void consumeDoubleIfAbove(DoubleConsumer consumer, double value, double threshold) {
    if (value > threshold) {
      consumer.accept(value);
    }
  }

  public static void consumeDoubleAndBooleanIfAbove(
      DoubleAndBooleanConsumer consumer,
      double value,
      boolean b,
      double threshold) {
    if (value > threshold) {
      consumer.accept(value, b);
    }
  }
}
