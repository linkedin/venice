package com.linkedin.venice.utils;

import com.linkedin.venice.exceptions.VeniceException;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;


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
   *
   * The list returned by this utility function should:
   * - be stored statically
   * - be accessed via {@link #valueOf(List, int, Class)}
   */
  public static <V extends VeniceEnumValue> List<V> getEnumValuesList(Class<V> enumToProvideArrayOf) {
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
    return Collections.unmodifiableList(Arrays.asList(array));
  }

  /**
   * This is a relaxed version of {@link #getEnumValuesList(Class)} which returns a map instead of a list.
   * This is useful when the values are not contiguous, or when the values are not starting from 0.
   */
  public static <V extends VeniceEnumValue> Map<Integer, V> getEnumValuesSparseList(Class<V> enumToProvideArrayOf) {
    String name = enumToProvideArrayOf.getSimpleName();
    Map<Integer, V> map = new HashMap<>();
    for (V type: enumToProvideArrayOf.getEnumConstants()) {
      if (map.put(type.getValue(), type) != null) {
        throw new IllegalStateException(name + " values must be unique!");
      }
    }
    return Collections.unmodifiableMap(map);
  }

  public static <V extends VeniceEnumValue> V valueOf(List<V> valuesList, int value, Class<V> enumClass) {
    return valueOf(valuesList, value, enumClass, VeniceException::new);
  }

  public static <V extends VeniceEnumValue> V valueOf(
      List<V> valuesList,
      int value,
      Class<V> enumClass,
      Function<String, VeniceException> exceptionConstructor) {
    try {
      return valuesList.get(value);
    } catch (IndexOutOfBoundsException e) {
      throw exceptionConstructor.apply("Invalid enum value for " + enumClass.getSimpleName() + ": " + value);
    }
  }

  public static <V extends VeniceEnumValue> V valueOf(Map<Integer, V> valuesMap, int value, Class<V> enumClass) {
    return valueOf(valuesMap, value, enumClass, VeniceException::new);
  }

  public static <V extends VeniceEnumValue> V valueOf(
      Map<Integer, V> valuesMap,
      int value,
      Class<V> enumClass,
      Function<String, VeniceException> exceptionConstructor) {
    if (!valuesMap.containsKey(value)) {
      throw exceptionConstructor.apply("Invalid enum value for " + enumClass.getSimpleName() + ": " + value);
    }
    return valuesMap.get(value);
  }
}
