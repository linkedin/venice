package com.linkedin.alpini.base.misc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public enum CollectionUtil {
  SINGLETON; // Effective Java, Item 3 - Enum singleton

  public static boolean isNotEmpty(Collection<?> collection) {
    return collection != null && !collection.isEmpty();
  }

  @SafeVarargs
  public static <T> Set<T> setOf(T... values) {
    if (values == null || values.length == 0) {
      return Collections.emptySet();
    } else if (values.length == 1) {
      return Collections.singleton(values[0]);
    } else {
      return Collections.unmodifiableSet(new HashSet<>(Arrays.asList(values)));
    }
  }

  public static <T> List<T> listOf(T value) {
    return Collections.singletonList(value);
  }

  @SafeVarargs
  public static <T> List<T> listOf(T... values) {
    if (values == null || values.length == 0) {
      return Collections.emptyList();
    } else if (values.length == 1) {
      return listOf(values[0]);
    } else {
      return listOf(Arrays.asList(values));
    }
  }

  public static <T> List<T> listOf(Collection<T> values) {
    if (values == null || values.isEmpty()) {
      return Collections.emptyList();
    } else {
      return Collections.unmodifiableList(new ArrayList<>(values));
    }
  }

  public static <K, V> Map<K, V> mapOf(K key, V value) {
    return Collections.singletonMap(key, value);
  }

  public static <T> Stream<T> stream(Iterator<? extends T> iterator) {
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false);
  }

  public static <T> T applyFactoryModifiers(T pipelineFactory, List<Function<T, T>> modifiers) {
    return modifiers.stream()
        .collect(() -> pipelineFactory, (factory, modifier) -> modifier.apply(factory), (ignore1, ignore2) -> {});
  }
}
