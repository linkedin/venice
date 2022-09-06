package com.linkedin.venice.utils;

import java.lang.reflect.Array;


public class EnumUtils {
  /**
   * Lots of defensive coding to ensure that control message type values are:
   *
   * 1. Unique
   * 2. From 0 to N
   * 3. Without gaps
   *
   * Checking these assumptions here helps to simplify valueOf(int) as much as possible, which is
   * valuable since it's a hot path call. If these assumptions change (e.g. if we deprecate some message
   * types such that there are gaps, then we may need to relax some constraints here and increase checks
   * in valueOf(int) instead.
   */
  public static <V extends VeniceEnumValue> V[] getEnumValuesArray(Class<V> enumToProvideArrayOf) {
    int maxValue = -1;
    String name = enumToProvideArrayOf.getSimpleName();
    for (V type: enumToProvideArrayOf.getEnumConstants()) {
      if (type.getValue() < 0) {
        throw new IllegalStateException("value cannot be < 0");
      }
      if (maxValue < type.getValue()) {
        maxValue = type.getValue();
      }
    }
    if (maxValue == -1) {
      throw new IllegalStateException("At least one " + name + " value must be defined!");
    }
    int neededArrayLength = maxValue + 1;
    V[] array = (V[]) Array.newInstance(enumToProvideArrayOf, neededArrayLength);
    for (V type: enumToProvideArrayOf.getEnumConstants()) {
      if (array[type.getValue()] != null) {
        throw new IllegalStateException(name + " values must be unique!");
      }
      array[type.getValue()] = type;
    }
    for (int i = 0; i < array.length; i++) {
      if (array[i] == null) {
        throw new IllegalStateException(
            name + " values should not have gaps, but " + i + " is not associated with any type!");
      }
    }
    return array;
  }
}
